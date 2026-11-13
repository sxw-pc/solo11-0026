from __future__ import annotations

import json

import pytest

from lakeback._util import canonical_json, sha256_text
from lakeback.cli import main
from lakeback.errors import LedgerError
from lakeback.lineage.store import LINEAGE_FILE
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.snapshotter import create_snapshot

SCHEMA = {"order_id": "int", "user_id": "int", "amount": "float", "status": "str", "dt": "str"}


def _snap(config, write_table, table, tag):
    rows = [
        {"order_id": 1, "user_id": 10, "amount": 4.5, "status": tag, "dt": "2024-02-01"},
        {"order_id": 2, "user_id": 11, "amount": 8.0, "status": tag, "dt": "2024-02-02"},
    ]
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["dt"], []).append(row)
    write_table(config.lake_root / table, grouped, SCHEMA)
    return create_snapshot(config, table)


def _entry(snapshot, sources, created_at="2025-01-01T00:00:00Z"):
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "table": snapshot["table"],
        "version": snapshot["version"],
        "sources": sources,
        "created_at": created_at,
    }


def _write_lineage(config, entries):
    """Write a lineage file with a valid hash chain but arbitrary entry content."""
    previous_hash = ""
    lines = []
    for entry in entries:
        record = {
            "entry": entry,
            "previous_hash": previous_hash,
            "record_hash": sha256_text(previous_hash + canonical_json(entry)),
        }
        lines.append(canonical_json(record))
        previous_hash = record["record_hash"]
    (config.ledger_dir / LINEAGE_FILE).write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_load_rejects_duplicate_snapshot_records(make_config, write_table):
    config = make_config()
    snap = _snap(config, write_table, "orders", "a")
    _write_lineage(config, [_entry(snap, []), _entry(snap, [])])
    with pytest.raises(LedgerError, match="duplicate lineage record"):
        Ledger(config.ledger_dir)


def test_load_rejects_unknown_snapshot_reference(make_config, write_table):
    config = make_config()
    _snap(config, write_table, "orders", "a")
    ghost = {"snapshot_id": "snap-" + "0" * 32, "table": "orders", "version": 1}
    _write_lineage(config, [_entry(ghost, [])])
    with pytest.raises(LedgerError, match="unknown snapshot"):
        Ledger(config.ledger_dir)


def test_load_rejects_mismatched_table_version(make_config, write_table):
    config = make_config()
    snap = _snap(config, write_table, "orders", "a")
    entry = _entry(snap, [])
    entry["version"] = 7
    _write_lineage(config, [entry])
    with pytest.raises(LedgerError, match="does not match snapshot"):
        Ledger(config.ledger_dir)


def test_load_rejects_non_canonical_sources(make_config, write_table):
    config = make_config()
    _snap(config, write_table, "orders", "a")
    _snap(config, write_table, "users", "u")
    snap = _snap(config, write_table, "orders_agg", "g")
    sources = [{"table": "users", "version": 1}, {"table": "orders", "version": 1}]
    _write_lineage(config, [_entry(snap, sources)])
    with pytest.raises(LedgerError, match="not canonical"):
        Ledger(config.ledger_dir)


@pytest.mark.parametrize(
    "sources",
    [
        [{"table": "orders", "version": 0}],
        [{"table": "orders", "version": "1"}],
        [{"table": "bad name!", "version": 1}],
        [{"table": "orders"}],
        [{"table": "orders", "version": 1, "extra": 1}],
    ],
)
def test_load_rejects_invalid_source_declarations(make_config, write_table, sources):
    config = make_config()
    snap = _snap(config, write_table, "orders", "a")
    _write_lineage(config, [_entry(snap, sources)])
    with pytest.raises(LedgerError, match="lineage"):
        Ledger(config.ledger_dir)


def test_load_rejects_broken_lineage_hash_chain(make_config, write_table):
    config = make_config()
    first = _snap(config, write_table, "orders", "a")
    second = _snap(config, write_table, "users", "u")
    _write_lineage(config, [_entry(first, []), _entry(second, [])])
    path = config.ledger_dir / LINEAGE_FILE
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    records[1]["previous_hash"] = "0" * 64
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    with pytest.raises(LedgerError, match="hash chain is broken"):
        Ledger(config.ledger_dir)


def _run(capsys, config, *arguments):
    code = main(["--config", str(config.path), *arguments])
    captured = capsys.readouterr()
    return code, captured.out, captured.err


@pytest.mark.parametrize(
    "arguments",
    [
        ("lineage", "register", "--table", "orders", "--at", "1", "--source", "orders"),
        ("lineage", "register", "--table", "orders", "--at", "1", "--source", "=2"),
        ("lineage", "register", "--table", "orders", "--at", "1", "--source", "orders=x"),
        ("lineage", "register", "--table", "orders", "--at", "1", "--source", "orders=0"),
        ("lineage", "upstream", "--table", "orders"),
        ("lineage", "downstream", "--table", "orders", "--at", "1", "--limit", "-2"),
    ],
)
def test_lineage_usage_errors_exit_two(make_config, write_table, capsys, arguments):
    config = make_config()
    _snap(config, write_table, "orders", "a")
    code, _, err = _run(capsys, config, *arguments)
    assert code == 2
    assert err.startswith("lakeback:")


def test_identifying_with_both_snapshot_and_table_fails(make_config, write_table, capsys):
    config = make_config()
    snap = _snap(config, write_table, "orders", "a")
    code, _, err = _run(
        capsys, config, "lineage", "upstream",
        "--snapshot", snap["snapshot_id"], "--table", "orders", "--at", "1",
    )
    assert code == 2
    assert "not both" in err
