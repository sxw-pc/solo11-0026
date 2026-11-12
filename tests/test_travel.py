from __future__ import annotations

import pytest

from lakeback.errors import BackendError, NotFoundError, RequestError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.snapshotter import create_snapshot
from lakeback.travel.reader import read_view
from lakeback.travel.resolver import resolve_snapshot

SCHEMA = {"order_id": "int", "amount": "float", "dt": "str"}
V1 = [
    {"order_id": 2, "amount": 2.0, "dt": "2024-02-01"},
    {"order_id": 10, "amount": 10.0, "dt": "2024-02-02"},
    {"order_id": 1, "amount": 1.0, "dt": "2024-02-01"},
]
V2 = [
    {"order_id": 2, "amount": 22.5, "dt": "2024-02-01"},
    {"order_id": 6, "amount": 6.0, "dt": "2024-02-02"},
    {"order_id": 1, "amount": 1.0, "dt": "2024-02-01"},
]


def _group(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["dt"], []).append(row)
    return grouped


def _pair(config, write_table):
    write_table(config.lake_root / "orders", _group(V1), SCHEMA)
    first = create_snapshot(config, "orders", clock=lambda: "2025-01-01T00:00:00Z")
    write_table(config.lake_root / "orders", _group(V2), SCHEMA)
    second = create_snapshot(config, "orders", clock=lambda: "2025-01-02T00:00:00Z")
    return first, second


def test_resolve_exact_version(make_config, write_table):
    config = make_config()
    first, _ = _pair(config, write_table)
    resolved = resolve_snapshot(Ledger(config.ledger_dir), "orders", "1")
    assert resolved["snapshot_id"] == first["snapshot_id"]


def test_time_travel_uses_latest_snapshot_at_or_before(make_config, write_table):
    config = make_config()
    first, second = _pair(config, write_table)
    ledger = Ledger(config.ledger_dir)
    assert resolve_snapshot(ledger, "orders", "2025-01-01T00:00:00Z")["version"] == 1
    assert resolve_snapshot(ledger, "orders", "2025-01-01T12:00:00Z")["version"] == 1
    assert resolve_snapshot(ledger, "orders", "2025-01-02T00:00:00Z")["version"] == 2
    assert resolve_snapshot(ledger, "orders", "2025-01-02T00:00:00Z")["snapshot_id"] == (
        second["snapshot_id"]
    )
    assert resolve_snapshot(ledger, "orders", "latest")["snapshot_id"] == second["snapshot_id"]
    assert resolve_snapshot(ledger, "orders", "v1")["snapshot_id"] == first["snapshot_id"]


def test_time_travel_before_earliest_is_not_found(make_config, write_table):
    config = make_config()
    _pair(config, write_table)
    with pytest.raises(NotFoundError, match="at or before"):
        resolve_snapshot(Ledger(config.ledger_dir), "orders", "2024-12-31T23:59:59Z")


@pytest.mark.parametrize("selector", ["3", "0", "not-a-version", "2025-99-99T00:00:00Z"])
def test_bad_or_missing_selector(make_config, write_table, selector):
    config = make_config()
    _pair(config, write_table)
    with pytest.raises((NotFoundError, RequestError)):
        resolve_snapshot(Ledger(config.ledger_dir), "orders", selector)


def test_read_view_returns_sorted_and_paginated_rows(make_config, write_table):
    config = make_config()
    first, _ = _pair(config, write_table)
    result = read_view(config, Ledger(config.ledger_dir), "orders", 1, limit=2, offset=1)
    assert result["snapshot"]["snapshot_id"] == first["snapshot_id"]
    assert result["total_rows"] == 3
    assert result["returned"] == 2
    assert [row["order_id"] for row in result["rows"]] == [2, 10]
    assert [row["order_id"] for row in read_view(
        config, Ledger(config.ledger_dir), "orders", 1
    )["rows"]] == [1, 2, 10]


def test_partition_pruning_reads_only_selected_files(make_config, write_table):
    config = make_config()
    first, _ = _pair(config, write_table)
    ledger = Ledger(config.ledger_dir)
    result = read_view(
        config, ledger, "orders", 1, partitions={"dt": "2024-02-01"}
    )
    assert result["total_rows"] == 2
    assert {row["dt"] for row in result["rows"]} == {"2024-02-01"}
    p2_file = next(item for item in first["files"] if item["partition"]["dt"] == "2024-02-02")
    (config.snapshot_store / first["snapshot_id"] / p2_file["path"]).unlink()
    assert read_view(config, ledger, "orders", 1, partitions={"dt": "2024-02-01"})["total_rows"] == 2
    with pytest.raises(NotFoundError, match="snapshot data file is missing"):
        read_view(config, ledger, "orders", 1)


def test_unknown_partition_is_a_request_error(make_config, write_table):
    config = make_config()
    _pair(config, write_table)
    with pytest.raises(RequestError, match="unknown partition"):
        read_view(config, Ledger(config.ledger_dir), "orders", 1, partitions={"region": "north"})


def test_snapshot_file_checksum_is_enforced(make_config, write_table):
    config = make_config()
    first, _ = _pair(config, write_table)
    stored = config.snapshot_store / first["snapshot_id"] / first["files"][0]["path"]
    stored.write_bytes(stored.read_bytes() + b"corruption")
    with pytest.raises(BackendError, match="checksum mismatch"):
        read_view(config, Ledger(config.ledger_dir), "orders", 1)


def test_pagination_validation(make_config, write_table):
    config = make_config()
    _pair(config, write_table)
    with pytest.raises(RequestError, match="offset"):
        read_view(config, Ledger(config.ledger_dir), "orders", 1, offset=-1)
    with pytest.raises(RequestError, match="limit"):
        read_view(config, Ledger(config.ledger_dir), "orders", 1, limit=-2)


def test_old_snapshot_survives_lake_overwrite(make_config, write_table):
    config = make_config()
    _pair(config, write_table)
    write_table(
        config.lake_root / "orders",
        {"2024-02-01": [{"order_id": 999, "amount": 1.0, "dt": "2024-02-01"}]},
        SCHEMA,
    )
    result = read_view(config, Ledger(config.ledger_dir), "orders", 1)
    assert 999 not in [row["order_id"] for row in result["rows"]]
    assert [row["order_id"] for row in result["rows"]] == [1, 2, 10]
