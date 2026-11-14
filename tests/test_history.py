from __future__ import annotations

import json

import pytest

from lakeback._util import tree_digest
from lakeback.cli import main
from lakeback.config import Config
from lakeback.errors import RequestError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.snapshotter import create_snapshot
from lakeback.travel.history import entity_history
from lakeback.travel.states import ABSENT, PRESENT, UNVERIFIABLE

SCHEMA = {"order_id": "int", "name": "str", "amount": "int", "dt": "str"}


def _rows(version: int) -> list[dict]:
    """A tiny history: modify, delete, then restore the original values."""
    if version == 1:
        return [
            {"order_id": 1, "name": "alpha", "amount": 5, "dt": "2024-02-01"},
            # NULL lives beside non-NULL values so CSV inference keeps amount=int.
            {"order_id": 2, "name": "beta", "amount": None, "dt": "2024-02-01"},
            {"order_id": 10, "name": "gamma", "amount": 1, "dt": "2024-02-01"},
        ]
    if version == 2:
        return [
            {"order_id": 1, "name": "alpha", "amount": 5, "dt": "2024-02-01"},
            {"order_id": 2, "name": "beta", "amount": 7, "dt": "2024-02-02"},
            {"order_id": 3, "name": "delta", "amount": 9, "dt": "2024-02-02"},
            {"order_id": 10, "name": "gamma", "amount": 1, "dt": "2024-02-01"},
        ]
    if version == 3:
        return [
            {"order_id": 2, "name": "beta", "amount": 7, "dt": "2024-02-02"},
            {"order_id": 10, "name": "gamma", "amount": 2, "dt": "2024-02-01"},
        ]
    # version 4 restores order 1 byte-for-byte with its version-1 values.
    return [
        {"order_id": 1, "name": "alpha", "amount": 5, "dt": "2024-02-01"},
        {"order_id": 2, "name": "beta", "amount": 7, "dt": "2024-02-02"},
        {"order_id": 10, "name": "gamma", "amount": 2, "dt": "2024-02-01"},
    ]


def _group(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["dt"], []).append(row)
    return grouped


def _build(config, write_table, timestamps=None) -> list[dict]:
    timestamps = timestamps or [
        "2025-01-01T00:00:00Z",
        "2025-01-02T00:00:00Z",
        "2025-01-03T00:00:00Z",
        "2025-01-04T00:00:00Z",
    ]
    manifests = []
    for index, version in enumerate((1, 2, 3, 4)):
        write_table(config.lake_root / "orders", _group(_rows(version)), SCHEMA)
        manifests.append(
            create_snapshot(config, "orders", clock=lambda t=timestamps[index]: t)
        )
    return manifests


def _history(config, **kwargs):
    kwargs.setdefault("key_value", 1)
    return entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 1, 4, **kwargs
    )


def _intervals(result):
    assert result["entity_count"] == 1
    return result["entities"][0]["intervals"]


def test_delete_and_restore_are_separate_intervals(make_config, write_table):
    config = make_config()
    manifests = _build(config, write_table)
    result = _history(config)
    assert result["status"] == "complete"
    assert result["complete"] is True
    assert [i["valid_from"] for i in _intervals(result)] == [1, 3, 4]
    assert [i["status"] for i in _intervals(result)] == [PRESENT, ABSENT, PRESENT]
    assert _intervals(result)[0]["valid_to"] == 3
    assert _intervals(result)[-1]["valid_to"] == 5
    assert _intervals(result)[0]["fields"] == _intervals(result)[2]["fields"]
    assert _intervals(result)[1]["fields"] is None
    # Source snapshot identity is retained on every interval.
    assert _intervals(result)[0]["source_snapshot"] == manifests[0]["snapshot_id"]
    assert _intervals(result)[1]["source_snapshot"] == manifests[2]["snapshot_id"]
    assert _intervals(result)[2]["source_snapshot"] == manifests[3]["snapshot_id"]


def test_null_field_is_not_the_same_as_an_absent_row(make_config, write_table):
    config = make_config()
    _build(config, write_table)
    present = _history(config, key_value=2)["entities"][0]["intervals"]
    assert [i["status"] for i in present] == [PRESENT, PRESENT]
    assert present[0]["fields"]["amount"] is None
    assert present[0]["present"] is True
    assert present[1]["fields"]["amount"] == 7

    gone = _history(config, key_value=3)["entities"][0]["intervals"]
    assert [i["status"] for i in gone] == [ABSENT, PRESENT, ABSENT]
    assert [i["valid_from"] for i in gone] == [1, 2, 3]


def test_same_second_snapshots_follow_version_order(make_config, write_table):
    config = make_config()
    _build(
        config, write_table,
        timestamps=["2025-01-01T00:00:00Z"] * 4,
    )
    manifests = Ledger(config.ledger_dir).snapshots("orders")
    assert [item["created_at"] for item in manifests] == ["2025-01-01T00:00:00Z"] * 4
    assert [item["version"] for item in manifests] == [1, 2, 3, 4]
    result = _history(config)
    assert [i["status"] for i in _intervals(result)] == [PRESENT, ABSENT, PRESENT]


def test_unknown_key_returns_empty_history(make_config, write_table):
    config = make_config()
    _build(config, write_table)
    result = _history(config, key_value=999)
    assert result["entities"] == []
    assert result["entity_count"] == 0
    assert result["status"] == "complete"


def test_batch_mode_sorts_by_primary_key_and_covers_range(make_config, write_table):
    config = make_config()
    _build(config, write_table)
    result = entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 1, 4
    )
    assert [entity["key"] for entity in result["entities"]] == [1, 2, 3, 10]
    # 10 only appears as a plain value change in v3.
    ten = result["entities"][-1]["intervals"]
    assert [(i["valid_from"], i["valid_to"], i["fields"]["amount"]) for i in ten] == [
        (1, 3, 1),
        (3, 5, 2),
    ]
    again = entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 1, 4
    )
    assert json.dumps(result, sort_keys=True) == json.dumps(again, sort_keys=True)


def test_identical_content_reuse_does_not_add_versions_or_intervals(make_config, write_table):
    config = make_config()
    _build(config, write_table)
    before = entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 1, 4
    )
    write_table(config.lake_root / "orders", _group(_rows(4)), SCHEMA)
    reused = create_snapshot(config, "orders", clock=lambda: "2030-01-01T00:00:00Z")
    assert reused["version"] == 4
    assert len(Ledger(config.ledger_dir).snapshots("orders")) == 4
    after = entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 1, 4
    )
    assert json.dumps(before, sort_keys=True) == json.dumps(after, sort_keys=True)


def _stored_file(config, manifest, predicate=None):
    items = manifest["files"]
    if predicate is not None:
        items = [item for item in items if predicate(item)]
    return config.snapshot_store / manifest["snapshot_id"] / items[0]["path"]


def test_checksum_mismatch_marks_that_version_unverifiable(make_config, write_table):
    config = make_config()
    manifests = _build(config, write_table)
    damaged = _stored_file(
        config, manifests[1], lambda item: item["partition"]["dt"] == "2024-02-01"
    )
    damaged.write_bytes(damaged.read_bytes() + b"corruption")
    result = _history(config)
    assert result["status"] == "incomplete"
    assert result["complete"] is False
    assert [item["version"] for item in result["unverifiable_versions"]] == [2]
    assert result["unverifiable_versions"][0]["snapshot_id"] == manifests[1]["snapshot_id"]
    intervals = _intervals(result)
    assert [i["status"] for i in intervals] == [
        PRESENT, UNVERIFIABLE, ABSENT, PRESENT,
    ]
    assert intervals[1]["valid_from"] == 2
    assert intervals[1]["valid_to"] == 3
    assert "checksum" in intervals[1]["error"]["message"]


def test_missing_data_file_is_unverifiable_not_deleted(make_config, write_table):
    config = make_config()
    manifests = _build(config, write_table)
    _stored_file(
        config, manifests[1], lambda item: item["partition"]["dt"] == "2024-02-02"
    ).unlink()
    result = _history(config)
    assert result["status"] == "incomplete"
    intervals = _intervals(result)
    assert [i["status"] for i in intervals] == [
        PRESENT, UNVERIFIABLE, ABSENT, PRESENT,
    ]
    assert intervals[1]["error"]["code"] == "not_found"
    assert "missing" in intervals[1]["error"]["message"]


def test_duplicate_primary_key_marks_version_unverifiable(make_config, write_table):
    config = make_config()
    write_table(config.lake_root / "orders", _group(_rows(1)), SCHEMA)
    create_snapshot(config, "orders", clock=lambda: "2025-01-01T00:00:00Z")
    duplicate = _rows(2) + [
        {"order_id": 1, "name": "clone", "amount": 8, "dt": "2024-02-02"}
    ]
    write_table(config.lake_root / "orders", _group(duplicate), SCHEMA)
    create_snapshot(config, "orders", clock=lambda: "2025-01-02T00:00:00Z")
    result = entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 1, 2,
        key_value=1,
    )
    assert result["status"] == "incomplete"
    assert [item["version"] for item in result["unverifiable_versions"]] == [2]
    assert "duplicate" in result["unverifiable_versions"][0]["error"]["message"]
    assert [i["status"] for i in _intervals(result)] == [PRESENT, UNVERIFIABLE]


def test_query_is_read_only(make_config, write_table):
    config = make_config()
    _build(config, write_table)
    ledger_bytes = (config.ledger_dir / "snapshots.jsonl").read_bytes()
    index_bytes = (config.ledger_dir / "index.json").read_bytes()
    digest = tree_digest(config.snapshot_store)
    _history(config)
    entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 1, 4
    )
    assert (config.ledger_dir / "snapshots.jsonl").read_bytes() == ledger_bytes
    assert (config.ledger_dir / "index.json").read_bytes() == index_bytes
    assert tree_digest(config.snapshot_store) == digest


def test_unknown_key_in_incomplete_range_reports_unknown(make_config, write_table):
    config = make_config()
    manifests = _build(config, write_table)
    _stored_file(
        config, manifests[1], lambda item: item["partition"]["dt"] == "2024-02-02"
    ).unlink()
    result = _history(config, key_value=999)
    assert result["status"] == "incomplete"
    assert result["entity_count"] == 1
    states = _intervals(result)
    # absent at v1, unknown at v2, absent again at v3-v4 (adjacent equals merge)
    assert [i["status"] for i in states] == [ABSENT, UNVERIFIABLE, ABSENT]
    assert [(i["valid_from"], i["valid_to"]) for i in states] == [(1, 2), (2, 3), (3, 5)]


def test_batch_mode_includes_entities_only_seen_in_later_versions(make_config, write_table):
    config = make_config()
    _build(config, write_table)
    result = entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 1, 2
    )
    assert [entity["key"] for entity in result["entities"]] == [1, 2, 3, 10]
    three = result["entities"][2]["intervals"]
    assert [(i["valid_from"], i["valid_to"]) for i in three] == [(1, 2), (2, 3)]
    assert [i["status"] for i in three] == [ABSENT, PRESENT]


def test_batch_mode_ignores_entities_outside_the_range(make_config, write_table):
    config = make_config()
    _build(config, write_table)
    result = entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 3, 3
    )
    # Entity 1 is absent in v3; it is not "seen" in the range.
    assert [entity["key"] for entity in result["entities"]] == [2, 10]
    assert result["from_version"] == 3 and result["to_version"] == 3


def test_latest_is_a_valid_range_boundary(make_config, write_table):
    config = make_config()
    _build(config, write_table)
    result = entity_history(
        config, Ledger(config.ledger_dir, read_only=True), "orders", 1, "latest",
        key_value=1,
    )
    assert result["to_version"] == 4
    assert _intervals(result)[-1]["valid_to"] == 5


def test_read_only_ledger_does_not_create_directories_or_index(tmp_path):
    config = Config.load(_minimal_config(tmp_path))
    ledger = Ledger(config.ledger_dir, read_only=True)
    assert ledger.snapshots("orders") == []
    assert not config.ledger_dir.exists()
    with pytest.raises(Exception, match="read-only"):
        ledger.append({"table": "orders"})


def _minimal_config(root):
    (root / "lakeback.toml").write_text(
        '[paths]\nlake = "lake"\nledger = "ledger"\nsnapshots = "snapshots"\n'
        'reports = "reports"\nfixtures = "fixtures"\nseed = "seed.toml"\n'
        '[storage]\nbackend = "csv"\n[keys]\norders = "order_id"\n',
        encoding="utf-8",
    )
    return root / "lakeback.toml"


def test_invalid_ranges_are_rejected(make_config, write_table):
    config = make_config()
    _build(config, write_table)
    ledger = Ledger(config.ledger_dir, read_only=True)
    with pytest.raises(RequestError, match="greater"):
        entity_history(config, ledger, "orders", 3, 2, key_value=1)
    with pytest.raises(RequestError, match="version"):
        entity_history(config, ledger, "orders", "2025-01-01T00:00:00Z", 4)
    from lakeback.errors import NotFoundError

    with pytest.raises(NotFoundError, match="version 9"):
        entity_history(config, ledger, "orders", 1, 9, key_value=1)


def _run(capsys, config, *arguments):
    code = main(["--config", str(config.path), *arguments])
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_history_cli_json_and_human_output(make_config, write_table, capsys):
    config = make_config()
    _build(config, write_table)
    code, out, err = _run(
        capsys, config, "history", "--table", "orders",
        "--from", "1", "--to", "4", "--key-value", "1", "--json",
    )
    assert code == 0, err
    payload = json.loads(out)
    assert payload["status"] == "complete"
    assert [i["status"] for i in payload["entities"][0]["intervals"]] == [
        PRESENT, ABSENT, PRESENT,
    ]
    code, out, _ = _run(
        capsys, config, "history", "--table", "orders", "--from", "1", "--to", "4"
    )
    assert code == 0
    assert "key=1" in out and "[1,3) present" in out and "[3,4) absent" in out


def test_history_cli_exit_codes(make_config, write_table, capsys):
    config = make_config()
    _build(config, write_table)
    code, _, err = _run(
        capsys, config, "history", "--table", "orders", "--from", "3", "--to", "2"
    )
    assert code == 2
    assert err.startswith("lakeback:")
    code, _, err = _run(
        capsys, config, "history", "--table", "orders", "--from", "1", "--to", "99"
    )
    assert code == 3
    assert "no snapshot version 99" in err
