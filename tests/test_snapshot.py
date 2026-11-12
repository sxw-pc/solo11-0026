from __future__ import annotations

import json

import pytest

from lakeback.errors import DataError, LedgerError, NotFoundError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.snapshotter import create_snapshot

SCHEMA = {"order_id": "int", "user_id": "int", "amount": "float", "status": "str", "dt": "str"}


def _rows(tag: str = "a"):
    return [
        {"order_id": 1, "user_id": 10, "amount": 4.5, "status": tag, "dt": "2024-02-01"},
        {"order_id": 2, "user_id": 11, "amount": 8.0, "status": tag, "dt": "2024-02-02"},
        {"order_id": 3, "user_id": 12, "amount": 9.25, "status": tag, "dt": "2024-02-01"},
    ]


def _table(config, write_table, rows=None):
    grouped: dict[str, list[dict]] = {}
    for row in rows or _rows():
        grouped.setdefault(row["dt"], []).append(row)
    return write_table(config.lake_root / "orders", grouped, SCHEMA)


def test_snapshot_is_idempotent_and_copies_files(make_config, write_table):
    config = make_config()
    _table(config, write_table)
    first = create_snapshot(config, "orders")
    second = create_snapshot(config, "orders")
    assert first["snapshot_id"] == second["snapshot_id"]
    assert first["version"] == second["version"] == 1
    assert first["row_count"] == 3
    assert first["file_count"] == 2
    assert first["created_at"] == "2025-01-01T00:00:00Z"
    assert first["parent_id"] is None
    assert first["files"][0]["path"] == "dt=2024-02-01/part-00000.csv"
    assert len(first["files"][0]["sha256"]) == 64
    stored = config.snapshot_store / first["snapshot_id"] / first["files"][0]["path"]
    assert stored.is_file()
    assert len(config.ledger_dir.joinpath("snapshots.jsonl").read_text().splitlines()) == 1
    assert json.loads((config.ledger_dir / "index.json").read_text())["record_count"] == 1


def test_new_content_creates_version_with_parent(make_config, write_table):
    config = make_config()
    _table(config, write_table)
    first = create_snapshot(config, "orders")
    changed = _rows("b")
    changed.append({"order_id": 4, "user_id": 13, "amount": 1.0, "status": "new", "dt": "2024-02-02"})
    grouped: dict[str, list[dict]] = {}
    for row in changed:
        grouped.setdefault(row["dt"], []).append(row)
    write_table(config.lake_root / "orders", grouped, SCHEMA)
    second = create_snapshot(config, "orders")
    assert second["version"] == 2
    assert second["parent_id"] == first["snapshot_id"]
    assert second["snapshot_id"] != first["snapshot_id"]
    assert [item["version"] for item in Ledger(config.ledger_dir).snapshots("orders")] == [1, 2]


def test_snapshot_id_is_independent_of_creation_time(make_config, write_table):
    config = make_config()
    _table(config, write_table)
    first = create_snapshot(config, "orders", clock=lambda: "2025-01-01T00:00:00Z")
    second = create_snapshot(config, "orders", clock=lambda: "2030-12-31T23:59:59Z")
    assert second["snapshot_id"] == first["snapshot_id"]
    assert second["created_at"] == first["created_at"]


def test_snapshot_missing_table(make_config):
    config = make_config()
    with pytest.raises(NotFoundError, match="table directory does not exist"):
        create_snapshot(config, "orders")


def test_snapshot_rejects_invalid_table_name(make_config):
    config = make_config()
    with pytest.raises(DataError, match="invalid table name"):
        create_snapshot(config, "../orders")


def test_ledger_detects_modified_history(make_config, write_table):
    config = make_config()
    _table(config, write_table)
    create_snapshot(config, "orders")
    ledger_path = config.ledger_dir / "snapshots.jsonl"
    record = json.loads(ledger_path.read_text(encoding="utf-8"))
    record["manifest"]["row_count"] = 999
    ledger_path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    with pytest.raises(LedgerError, match="modified"):
        Ledger(config.ledger_dir)


def test_ledger_detects_index_tampering(make_config, write_table):
    config = make_config()
    _table(config, write_table)
    create_snapshot(config, "orders")
    index_path = config.ledger_dir / "index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    index["record_count"] = 99
    index_path.write_text(json.dumps(index), encoding="utf-8")
    with pytest.raises(LedgerError, match="index is inconsistent"):
        Ledger(config.ledger_dir)


def test_ledger_get_version_and_snapshot(make_config, write_table):
    config = make_config()
    _table(config, write_table)
    manifest = create_snapshot(config, "orders")
    ledger = Ledger(config.ledger_dir)
    assert ledger.get_version("orders", 1)["snapshot_id"] == manifest["snapshot_id"]
    assert ledger.get_snapshot(manifest["snapshot_id"])["table"] == "orders"
    assert ledger.tables()["orders"]["latest_version"] == 1
    assert [item["table"] for item in ledger.snapshots()] == ["orders"]
    with pytest.raises(NotFoundError):
        ledger.get_version("orders", 2)
    with pytest.raises(NotFoundError):
        ledger.get_snapshot("snap-" + "0" * 32)
    ledger.verify()
