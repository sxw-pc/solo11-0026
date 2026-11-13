from __future__ import annotations

import json

import pytest

from lakeback.errors import BackendError, DataError, LedgerError, NotFoundError, RequestError
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
    with pytest.raises(NotFoundError, match="at or before") as caught:
        resolve_snapshot(Ledger(config.ledger_dir), "orders", "2024-12-31T23:59:59Z")
    assert caught.value.details["earliest_version"] == 1
    assert caught.value.details["earliest_snapshot"] == "2025-01-01T00:00:00Z"


@pytest.mark.parametrize(
    "selector",
    [
        "2025-01-01T00:00:00Z",
        "2025-01-01T00:00:00+00:00",
        "2025-01-01T00:00:00",
        "2025-01-01T08:00:00+08:00",
        "2025-01-01T00:00:00.000Z",
        "2025-01-01T02:00:00+02:00",
    ],
)
def test_equivalent_iso_spellings_resolve_the_same_snapshot(make_config, write_table, selector):
    config = make_config()
    first, _ = _pair(config, write_table)
    resolved = resolve_snapshot(Ledger(config.ledger_dir), "orders", selector)
    assert resolved["version"] == 1
    assert resolved["snapshot_id"] == first["snapshot_id"]


def test_same_second_snapshots_pick_the_higher_version(make_config, write_table):
    config = make_config()
    write_table(config.lake_root / "orders", _group(V1), SCHEMA)
    first = create_snapshot(config, "orders", clock=lambda: "2025-01-01T00:00:00Z")
    write_table(config.lake_root / "orders", _group(V2), SCHEMA)
    second = create_snapshot(config, "orders", clock=lambda: "2025-01-01T00:00:00Z")
    assert (first["version"], second["version"]) == (1, 2)
    ledger = Ledger(config.ledger_dir)
    resolved = resolve_snapshot(ledger, "orders", "2025-01-01T00:00:00Z")
    assert resolved["snapshot_id"] == second["snapshot_id"]
    assert resolved["version"] == 2
    # The same second expressed through other offset spellings agrees.
    for selector in ("2025-01-01T08:00:00+08:00", "2025-01-01T00:00:00", "2025-01-01T00:00:00.999Z"):
        assert resolve_snapshot(ledger, "orders", selector)["version"] == 2


def test_out_of_range_timestamp_clamps_to_boundary(make_config, write_table):
    config = make_config()
    first, _ = _pair(config, write_table)
    ledger = Ledger(config.ledger_dir)
    # An instant below the representable calendar means "before everything".
    with pytest.raises(NotFoundError, match="at or before"):
        resolve_snapshot(ledger, "orders", "0001-01-01T00:00:00+14:00")
    # One above it means "far future" and resolves to the latest snapshot.
    assert resolve_snapshot(ledger, "orders", "9999-12-31T23:59:59-14:00")["version"] == 2
    assert first["version"] == 1


def test_historical_snapshot_row_count_and_schema_match_manifest(make_config, write_table):
    config = make_config()
    first, second = _pair(config, write_table)
    ledger = Ledger(config.ledger_dir)
    for manifest in (first, second):
        view = read_view(config, ledger, "orders", manifest["version"])
        assert view["total_rows"] == manifest["row_count"]
        assert view["schema"] == {item["name"]: item["type"] for item in manifest["schema"]}
    # A later partition is not visible when travelling to an older version.
    assert read_view(
        config, ledger, "orders", first["version"], partitions={"dt": "2024-02-02"}
    )["total_rows"] == 1
    assert read_view(
        config, ledger, "orders", first["version"], partitions={"dt": "2024-02-03"}
    )["total_rows"] == 0


def test_travel_detects_snapshot_row_count_tampering(make_config, write_table):
    config = make_config()
    _pair(config, write_table)
    record_path = config.ledger_dir / "snapshots.jsonl"
    lines = record_path.read_text(encoding="utf-8").splitlines()
    # Rewriting the ledger is caught by its hash chain before travel runs.
    record = json.loads(lines[0])
    record["manifest"]["files"][0]["rows"] += 1
    record_path.write_text(
        "\n".join([json.dumps(record), *lines[1:]]) + "\n", encoding="utf-8"
    )
    with pytest.raises(LedgerError):
        Ledger(config.ledger_dir)


def test_stored_copy_mismatch_is_detected_without_touching_ledger(make_config, write_table):
    """A snapshot copy that no longer matches the manifest fails loudly."""
    config = make_config()
    first, _ = _pair(config, write_table)
    stored = config.snapshot_store / first["snapshot_id"] / first["files"][0]["path"]
    # Re-read the file, rewrite it with an extra row, so the byte checksum and
    # the row count both disagree with the manifest.
    import csv
    with stored.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))
    header, body = rows[0], rows[1:]
    with stored.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows([*body, body[0]])
    with pytest.raises(BackendError, match="checksum mismatch"):
        read_view(config, Ledger(config.ledger_dir), "orders", first["version"])


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
