from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

import pytest

from lakeback.cli import main
from lakeback.errors import LedgerError
from lakeback.lineage.store import LineageStore
from lakeback.service.app import make_server
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.snapshotter import create_snapshot

SCHEMA = {"order_id": "int", "user_id": "int", "amount": "float", "status": "str", "dt": "str"}


def _run(capsys, config, *arguments):
    code = main(["--config", str(config.path), *arguments])
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _json_run(capsys, config, *arguments):
    code, out, err = _run(capsys, config, *arguments, "--json")
    payload = json.loads(out) if out.strip() else None
    return code, payload, err


def _write(config, write_table, table, tag):
    rows = [
        {"order_id": 1, "user_id": 10, "amount": 4.5, "status": tag, "dt": "2024-02-01"},
        {"order_id": 2, "user_id": 11, "amount": 8.0, "status": tag, "dt": "2024-02-02"},
    ]
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["dt"], []).append(row)
    return write_table(config.lake_root / table, grouped, SCHEMA)


def _snap(config, write_table, table, tag):
    _write(config, write_table, table, tag)
    return create_snapshot(config, table)


def _register(config, snapshot, sources):
    store = LineageStore(config.ledger_dir, Ledger(config.ledger_dir))
    return store.register(snapshot["snapshot_id"], sources, created_at=config.now_iso())


@pytest.fixture
def chains(make_config, write_table):
    """Three chains: orders->orders_agg->orders_daily, orders->user_revenue, users->users_agg."""
    config = make_config()
    snap = {}
    _snap(config, write_table, "orders", "v1")
    snap["orders2"] = _snap(config, write_table, "orders", "v2")
    snap["users1"] = _snap(config, write_table, "users", "u1")
    snap["agg1"] = _snap(config, write_table, "orders_agg", "a1")
    snap["revenue1"] = _snap(config, write_table, "user_revenue", "r1")
    snap["daily1"] = _snap(config, write_table, "orders_daily", "d1")
    snap["users_agg1"] = _snap(config, write_table, "users_agg", "ua1")
    _register(config, snap["agg1"], [{"table": "orders", "version": 2}])
    _register(config, snap["revenue1"], [{"table": "orders", "version": 2}])
    _register(config, snap["daily1"], [
        {"table": "orders_agg", "version": 1},
        {"table": "user_revenue", "version": 1},
    ])
    _register(config, snap["users_agg1"], [{"table": "users", "version": 1}])
    return config, snap


def test_upstream_lists_full_chain_with_depths(chains, capsys):
    config, snap = chains
    code, payload, _ = _json_run(
        capsys, config, "lineage", "upstream", "--table", "orders_daily", "--at", "1"
    )
    assert code == 0
    assert payload["root"]["snapshot_id"] == snap["daily1"]["snapshot_id"]
    assert payload["truncated"] is False
    by_table = {entry["table"]: entry for entry in payload["entries"]}
    assert by_table["orders_agg"]["depth"] == 1
    assert by_table["user_revenue"]["depth"] == 1
    assert by_table["orders"]["depth"] == 2
    assert by_table["orders"]["version"] == 2
    assert by_table["orders"]["status"] == "resolved"
    assert set(by_table["orders"]["via"]) == {
        snap["agg1"]["snapshot_id"], snap["revenue1"]["snapshot_id"]
    }


def test_downstream_marks_each_depth_level(chains, capsys):
    config, _ = chains
    code, payload, _ = _json_run(
        capsys, config, "lineage", "downstream", "--table", "orders", "--at", "2"
    )
    assert code == 0
    depths = {entry["table"]: entry["depth"] for entry in payload["entries"]}
    assert depths == {"orders_agg": 1, "user_revenue": 1, "orders_daily": 2}
    assert payload["total_count"] == 3
    assert payload["truncated"] is False
    code, payload, _ = _json_run(
        capsys, config, "lineage", "downstream", "--table", "users", "--at", "1"
    )
    assert code == 0
    assert [entry["table"] for entry in payload["entries"]] == ["users_agg"]


def test_downstream_limit_marks_truncation(chains, capsys):
    config, _ = chains
    code, payload, _ = _json_run(
        capsys, config, "lineage", "downstream",
        "--table", "orders", "--at", "2", "--limit", "2",
    )
    assert code == 0
    assert payload["entry_count"] == 2
    assert payload["total_count"] == 3
    assert payload["truncated"] is True
    assert payload["limit"] == 2


def test_cycle_is_rejected_with_snapshot_ids(make_config, write_table, capsys):
    config = make_config()
    alpha = _snap(config, write_table, "alpha", "a")
    beta = _snap(config, write_table, "beta", "b")
    _register(config, alpha, [{"table": "beta", "version": 1}])
    _register(config, beta, [{"table": "alpha", "version": 1}])
    for sub in ("upstream", "downstream"):
        code, out, err = _run(capsys, config, "lineage", sub, "--table", "alpha", "--at", "1")
        assert code == 5
        assert out == ""
        assert "cycle" in err
        assert alpha["snapshot_id"] in err and beta["snapshot_id"] in err
    code, _, err = _run(
        capsys, config, "lineage", "upstream", "--table", "alpha", "--at", "1", "--json"
    )
    assert code == 5
    payload = json.loads(err)
    assert payload["error"]["code"] == "lineage_cycle"
    assert set(payload["error"]["details"]["cycle"]) == {
        alpha["snapshot_id"], beta["snapshot_id"]
    }


def test_self_dependency_is_a_cycle(make_config, write_table, capsys):
    config = make_config()
    loop = _snap(config, write_table, "loop", "x")
    _register(config, loop, [{"table": "loop", "version": 1}])
    code, _, err = _run(capsys, config, "lineage", "upstream", "--table", "loop", "--at", "1")
    assert code == 5
    assert loop["snapshot_id"] in err


def test_dangling_source_is_explicitly_marked(make_config, write_table, capsys):
    config = make_config()
    orphan = _snap(config, write_table, "orphan", "o")
    code, payload, _ = _json_run(
        capsys, config, "lineage", "register",
        "--table", "orphan", "--at", "1", "--source", "ghost=9",
    )
    assert code == 0
    assert payload["unresolved"] == [{"table": "ghost", "version": 9}]
    code, payload, _ = _json_run(
        capsys, config, "lineage", "upstream", "--table", "orphan", "--at", "1"
    )
    assert code == 0
    assert payload["entries"] == [{
        "depth": 1, "table": "ghost", "version": 9, "snapshot_id": None,
        "status": "dangling", "declared": False, "via": [orphan["snapshot_id"]],
    }]
    code, _, err = _run(capsys, config, "lineage", "downstream", "--table", "ghost", "--at", "9")
    assert code == 3
    assert "has no snapshots" in err


def test_repeat_registration_creates_no_duplicates(make_config, write_table, capsys):
    config = make_config()
    _snap(config, write_table, "orders", "v1")
    _snap(config, write_table, "orders_agg", "a")
    args = ("lineage", "register", "--table", "orders_agg", "--at", "1", "--source", "orders=1")
    code, first, _ = _json_run(capsys, config, *args)
    assert code == 0 and first["created"] is True
    code, second, _ = _json_run(capsys, config, *args)
    assert code == 0 and second["created"] is False
    code, third, _ = _json_run(
        capsys, config, "lineage", "register", "--table", "orders_agg", "--at", "1",
        "--source", "orders=1", "--source", "orders=1",
    )
    assert code == 0 and third["created"] is False
    lines = (config.ledger_dir / "lineage.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1


def test_conflicting_registration_is_rejected(make_config, write_table, capsys):
    config = make_config()
    _snap(config, write_table, "orders", "v1")
    _snap(config, write_table, "orders", "v2")
    _snap(config, write_table, "orders_agg", "a")
    code, _, _ = _json_run(
        capsys, config, "lineage", "register",
        "--table", "orders_agg", "--at", "1", "--source", "orders=1",
    )
    assert code == 0
    code, _, err = _run(
        capsys, config, "lineage", "register",
        "--table", "orders_agg", "--at", "1", "--source", "orders=2",
    )
    assert code == 2
    assert "different sources" in err


def test_tampered_lineage_fails_at_load_time(make_config, write_table, capsys):
    config = make_config()
    _snap(config, write_table, "orders", "v1")
    _snap(config, write_table, "orders_agg", "a")
    code, _, _ = _json_run(
        capsys, config, "lineage", "register",
        "--table", "orders_agg", "--at", "1", "--source", "orders=1",
    )
    assert code == 0
    path = config.ledger_dir / "lineage.jsonl"
    text = path.read_text(encoding="utf-8")
    assert '"table":"orders"' in text
    path.write_text(text.replace('"table":"orders"', '"table":"orderz"', 1), encoding="utf-8")
    with pytest.raises(LedgerError, match="lineage record was modified"):
        Ledger(config.ledger_dir)
    code, _, err = _run(capsys, config, "travel", "--table", "orders", "--at", "1")
    assert code == 2
    assert "lineage record was modified" in err


def test_lineage_stays_pinned_when_upstream_adds_versions(make_config, write_table, capsys):
    config = make_config()
    _snap(config, write_table, "orders", "v1")
    _snap(config, write_table, "orders", "v2")
    _snap(config, write_table, "orders_agg", "a")
    code, _, _ = _json_run(
        capsys, config, "lineage", "register",
        "--table", "orders_agg", "--at", "1", "--source", "orders=2",
    )
    assert code == 0
    _snap(config, write_table, "orders", "v3")
    code, payload, _ = _json_run(
        capsys, config, "lineage", "upstream", "--table", "orders_agg", "--at", "1"
    )
    assert code == 0
    assert [(e["table"], e["version"]) for e in payload["entries"]] == [("orders", 2)]
    code, payload, _ = _json_run(
        capsys, config, "lineage", "downstream", "--table", "orders", "--at", "2"
    )
    assert [entry["table"] for entry in payload["entries"]] == ["orders_agg"]
    code, payload, _ = _json_run(
        capsys, config, "lineage", "downstream", "--table", "orders", "--at", "3"
    )
    assert code == 0 and payload["entries"] == []


def test_snapshot_source_declares_lineage_at_creation(make_config, write_table, capsys):
    config = make_config()
    _snap(config, write_table, "orders", "v1")
    _write(config, write_table, "orders_agg", "a")
    code, payload, _ = _json_run(
        capsys, config, "snapshot", "--table", "orders_agg", "--source", "orders=1"
    )
    assert code == 0
    assert payload["lineage"]["sources"] == [{"table": "orders", "version": 1}]
    assert payload["lineage"]["created"] is True
    code, payload, _ = _json_run(
        capsys, config, "snapshot", "--table", "orders_agg", "--source", "orders=1"
    )
    assert code == 0
    assert payload["lineage"]["created"] is False
    ledger_lines = (config.ledger_dir / "snapshots.jsonl").read_text().splitlines()
    lineage_lines = (config.ledger_dir / "lineage.jsonl").read_text().splitlines()
    assert len(ledger_lines) == 2
    assert len(lineage_lines) == 1


def test_empty_sources_declare_a_source_table(make_config, write_table, capsys):
    config = make_config()
    _snap(config, write_table, "orders", "v1")
    code, payload, _ = _json_run(
        capsys, config, "lineage", "register", "--table", "orders", "--at", "1"
    )
    assert code == 0
    assert payload["sources"] == []
    code, payload, _ = _json_run(
        capsys, config, "lineage", "upstream", "--table", "orders", "--at", "1"
    )
    assert code == 0
    assert payload["entries"] == []
    assert payload["root"]["declared"] is True


def test_undeclared_snapshot_is_an_implicit_source(make_config, write_table, capsys):
    config = make_config()
    _snap(config, write_table, "orders", "v1")
    code, payload, _ = _json_run(
        capsys, config, "lineage", "upstream", "--table", "orders", "--at", "1"
    )
    assert code == 0
    assert payload["entries"] == []
    assert payload["root"]["declared"] is False


def test_missing_version_exits_three(make_config, write_table, capsys):
    config = make_config()
    _snap(config, write_table, "orders", "v1")
    code, _, err = _run(capsys, config, "lineage", "upstream", "--table", "orders", "--at", "99")
    assert code == 3
    assert "no snapshot version 99" in err
    code, _, _ = _run(capsys, config, "lineage", "downstream", "--snapshot", "snap-" + "0" * 32)
    assert code == 3
    code, _, _ = _run(capsys, config, "lineage", "register", "--snapshot", "not-a-snapshot")
    assert code == 2


def _get_json(base, path):
    try:
        with urllib.request.urlopen(base + path) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


@pytest.fixture
def lineage_service(chains):
    config, snap = chains
    server = make_server(config, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}", snap
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_lineage_endpoints(lineage_service):
    base, snap = lineage_service
    status, payload = _get_json(base, "/api/lineage/upstream?table=orders_daily&at=1")
    assert status == 200
    assert payload["entry_count"] == 3
    status, payload = _get_json(base, "/api/lineage/downstream?table=orders&at=2&limit=1")
    assert status == 200
    assert payload["truncated"] is True
    assert payload["entry_count"] == 1
    assert payload["total_count"] == 3
    status, payload = _get_json(
        base, f"/api/lineage/upstream?snapshot={snap['daily1']['snapshot_id']}"
    )
    assert status == 200
    assert payload["root"]["table"] == "orders_daily"


def test_lineage_endpoint_errors(lineage_service):
    base, _ = lineage_service
    status, payload = _get_json(base, "/api/lineage/upstream?table=orders&at=99")
    assert status == 404
    assert payload["error"]["code"] == "not_found"
    status, payload = _get_json(base, "/api/lineage/upstream")
    assert status == 400
    assert payload["error"]["code"] == "bad_request"
    status, payload = _get_json(base, "/api/lineage/downstream?table=orders&at=2&limit=-1")
    assert status == 400


def test_lineage_cycle_over_http(make_config, write_table):
    config = make_config()
    alpha = _snap(config, write_table, "alpha", "a")
    beta = _snap(config, write_table, "beta", "b")
    _register(config, alpha, [{"table": "beta", "version": 1}])
    _register(config, beta, [{"table": "alpha", "version": 1}])
    server = make_server(config, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        status, payload = _get_json(
            f"http://{host}:{port}", "/api/lineage/upstream?table=alpha&at=1"
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    assert status == 400
    assert payload["error"]["code"] == "lineage_cycle"
    assert alpha["snapshot_id"] in payload["error"]["message"]
