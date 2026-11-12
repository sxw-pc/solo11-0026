from __future__ import annotations

import json

import pytest

from lakeback.errors import DataError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.snapshotter import create_snapshot
from lakeback.travel.diff import compute_diff, diff_versions

SCHEMA = {"order_id": "int", "amount": "float", "status": "str", "dt": "str"}
V1 = [
    {"order_id": 2, "amount": 2.0, "status": "paid", "dt": "2024-02-01"},
    {"order_id": 10, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
    {"order_id": 30, "amount": 30.0, "status": "pending", "dt": "2024-02-02"},
    {"order_id": 100, "amount": 100.0, "status": "paid", "dt": "2024-02-02"},
]
V2 = [
    {"order_id": 2, "amount": 2.0, "status": "paid", "dt": "2024-02-01"},
    {"order_id": 4, "amount": 4.0, "status": "new", "dt": "2024-02-01"},
    {"order_id": 10, "amount": 10.5, "status": "refunded", "dt": "2024-02-01"},
    {"order_id": 20, "amount": 20.0, "status": "new", "dt": "2024-02-02"},
    {"order_id": 550, "amount": 550.0, "status": "new", "dt": "2024-02-02"},
]


def _group(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["dt"], []).append(row)
    return grouped


def _snapshots(config, write_table):
    write_table(config.lake_root / "orders", _group(V1), SCHEMA)
    create_snapshot(config, "orders", clock=lambda: "2025-01-01T00:00:00Z")
    write_table(config.lake_root / "orders", _group(V2), SCHEMA)
    create_snapshot(config, "orders", clock=lambda: "2025-01-02T00:00:00Z")


def test_diff_counts_and_numeric_key_ordering(make_config, write_table):
    config = make_config()
    _snapshots(config, write_table)
    result = diff_versions(config, Ledger(config.ledger_dir), "orders", 1, 2)
    assert result["counts"] == {
        "from_rows": 4,
        "to_rows": 5,
        "added": 3,
        "removed": 2,
        "modified": 1,
        "unchanged": 1,
    }
    assert [row["order_id"] for row in result["added"]] == [4, 20, 550]
    assert [row["order_id"] for row in result["removed"]] == [30, 100]
    assert [item["key"] for item in result["modified"]] == [10]
    assert result["from"]["version"] == 1
    assert result["to"]["version"] == 2
    assert result["schema_changed"] is False


def test_diff_has_field_level_modification_details(make_config, write_table):
    config = make_config()
    _snapshots(config, write_table)
    result = diff_versions(config, Ledger(config.ledger_dir), "orders", 1, 2)
    changes = {
        change["name"]: change for change in result["modified"][0]["fields"]
    }
    assert changes["amount"]["before"] == 10.0
    assert changes["amount"]["after"] == 10.5
    assert changes["status"]["before"] == "paid"
    assert changes["status"]["after"] == "refunded"


def test_diff_is_byte_for_byte_deterministic(make_config, write_table):
    config = make_config()
    _snapshots(config, write_table)
    ledger = Ledger(config.ledger_dir)
    first = diff_versions(config, ledger, "orders", 1, 2)
    second = diff_versions(config, ledger, "orders", "v1", "2025-01-02T00:00:00Z")
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_diff_same_snapshot_is_empty(make_config, write_table):
    config = make_config()
    _snapshots(config, write_table)
    result = diff_versions(config, Ledger(config.ledger_dir), "orders", 2, 2)
    assert result["counts"]["added"] == result["counts"]["removed"] == result["counts"]["modified"] == 0
    assert result["counts"]["unchanged"] == 5


def test_compute_diff_requires_key_in_both_schemas():
    manifest = {"version": 1, "snapshot_id": "snap-" + "a" * 32,
                "created_at": "2025-01-01T00:00:00Z", "row_count": 0}
    with pytest.raises(DataError, match="primary key"):
        compute_diff("orders", manifest, manifest, [], [], "order_id", {"x": "int"}, {"x": "int"})


def test_diff_schema_change_is_reported(make_config, write_table):
    config = make_config()
    write_table(config.lake_root / "orders", _group(V1), SCHEMA)
    create_snapshot(config, "orders", clock=lambda: "2025-01-01T00:00:00Z")
    changed_schema = {"order_id": "int", "amount": "float", "status": "str", "dt": "str", "note": "str"}
    changed = [{**row, "note": "backfilled"} for row in V1]
    write_table(config.lake_root / "orders", _group(changed), changed_schema)
    create_snapshot(config, "orders", clock=lambda: "2025-01-02T00:00:00Z")
    result = diff_versions(config, Ledger(config.ledger_dir), "orders", 1, 2)
    assert result["schema_changed"] is True
    field_names = [change["name"] for change in result["modified"][0]["fields"]]
    assert "note" in field_names
