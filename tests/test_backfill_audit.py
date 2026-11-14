from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakeback._util import REPORT_RE
from lakeback.backends import ParquetBackend, get_backend
from lakeback.backfill import auditor as auditor_module
from lakeback.backfill.auditor import (
    CROSS_BACKEND_CONFLICT_STATUS,
    run_audit,
    run_cross_backend_audit,
)
from lakeback.backfill.report import load_report, render_markdown
from lakeback.errors import NotFoundError, RequestError

PARQUET = ParquetBackend.available()
requires_parquet = pytest.mark.skipif(
    not PARQUET, reason="cross-backend equivalence needs the parquet extra"
)

SCHEMA = {"order_id": "int", "amount": "float", "status": "str", "dt": "str"}
ROWS = [
    {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
    {"order_id": 2, "amount": 20.0, "status": "paid", "dt": "2024-02-01"},
    {"order_id": 3, "amount": 30.0, "status": "pending", "dt": "2024-02-02"},
]


def _group(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["dt"], []).append(row)
    return grouped


def _roots(config, write_table, before_rows, after_rows, after_schema=None):
    before = write_table(config.project_root / "before" / "orders", _group(before_rows), SCHEMA)
    after = write_table(
        config.project_root / "after" / "orders",
        _group(after_rows),
        after_schema or SCHEMA,
    )
    return before, after


def test_consistent_audit(make_config, write_table):
    config = make_config()
    before, after = _roots(config, write_table, ROWS, ROWS)
    report = run_audit(config, "orders", before, after)
    assert report["status"] == "consistent"
    assert all(check["status"] == "pass" for check in report["checks"])
    assert report["counts"]["modified_rows"] == 0
    assert report["counts"]["missing_in_after"] == 0
    assert report["residual"]["missing_in_after"] == []
    assert REPORT_RE.fullmatch(report["report_id"])
    assert report["checks"][0]["name"] == "schema"
    assert report["checks"][0]["evidence"]["differences"]["missing_fields"] == []
    assert report["artifacts"]["json_report"].endswith(".json")
    assert report["artifacts"]["markdown_report"].endswith(".md")


def test_residual_diff_audit(make_config, write_table):
    config = make_config()
    after_rows = [
        {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
        {"order_id": 2, "amount": 25.5, "status": "refunded", "dt": "2024-02-01"},
        {"order_id": 5, "amount": 50.0, "status": "new", "dt": "2024-02-02"},
    ]
    before, after = _roots(config, write_table, ROWS, after_rows)
    report = run_audit(config, "orders", before, after)
    assert report["status"] == "residual_diff"
    assert report["counts"]["missing_in_after"] == 1
    assert report["counts"]["extra_in_after"] == 1
    assert report["counts"]["modified_rows"] == 1
    assert report["residual"]["missing_in_after"] == [3]
    assert report["residual"]["extra_in_after"] == [5]
    changes = {item["name"]: item for item in report["residual"]["modified_rows"][0]["fields"]}
    assert changes["amount"] == {"name": "amount", "before": 20.0, "after": 25.5}
    assert changes["status"]["after"] == "refunded"
    check_map = {check["name"]: check["status"] for check in report["checks"]}
    assert check_map["schema"] == "pass"
    assert check_map["partition_structure"] == "pass"
    assert check_map["primary_key_set"] == "fail"
    assert check_map["partition_checksums"] == "fail"
    assert report["partition_checksum_mismatches"]


def test_structural_mismatch_from_schema(make_config, write_table):
    config = make_config()
    after_schema = {"order_id": "int", "amount": "int", "status": "str", "dt": "str"}
    after_rows = [{**row, "amount": int(row["amount"])} for row in ROWS]
    before, after = _roots(config, write_table, ROWS, after_rows, after_schema)
    report = run_audit(config, "orders", before, after)
    assert report["status"] == "structural_mismatch"
    schema_check = next(check for check in report["checks"] if check["name"] == "schema")
    assert schema_check["status"] == "fail"
    assert schema_check["evidence"]["differences"]["type_changes"]["amount"] == {
        "before": "float", "after": "int"
    }


def test_structural_mismatch_from_missing_partition(make_config, write_table):
    config = make_config()
    after_rows = [row for row in ROWS if row["dt"] == "2024-02-01"]
    before, after = _roots(config, write_table, ROWS, after_rows)
    report = run_audit(config, "orders", before, after)
    assert report["status"] == "structural_mismatch"
    partition_check = next(
        check for check in report["checks"] if check["name"] == "partition_structure"
    )
    assert partition_check["status"] == "fail"
    assert report["residual"]["missing_partitions"] == [{"dt": "2024-02-02"}]


def test_report_is_deterministic_and_loadable(make_config, write_table):
    config = make_config()
    before, after = _roots(config, write_table, ROWS, ROWS)
    first = run_audit(config, "orders", before, after)
    second = run_audit(config, "orders", before, after)
    assert first["report_id"] == second["report_id"]
    loaded = load_report(config.reports_dir, first["report_id"])
    assert loaded["status"] == "consistent"
    assert loaded["checks"] == first["checks"]
    markdown = (config.reports_dir / f"{first['report_id']}.md").read_text(encoding="utf-8")
    assert "**consistent**" in markdown
    assert render_markdown(loaded).startswith("# Backfill Audit Report: orders")
    json_payload = json.loads((config.reports_dir / f"{first['report_id']}.json").read_text())
    assert json_payload["report_id"] == first["report_id"]


def test_load_report_validation(make_config):
    config = make_config()
    with pytest.raises(RequestError, match="report_id"):
        load_report(config.reports_dir, "../../secret")
    with pytest.raises(NotFoundError, match="not found"):
        load_report(config.reports_dir, "rpt-" + "a" * 16)


def test_audit_requires_existing_roots(make_config):
    config = make_config()
    with pytest.raises(NotFoundError, match="before table directory"):
        run_audit(config, "orders", config.project_root / "missing", config.project_root)


# ---------------------------------------------------------------------------
# Cross-backend equivalence audit
# ---------------------------------------------------------------------------

CROSS_SCHEMA = {"order_id": "int", "amount": "float", "status": "str", "dt": "str"}


def _write_dual_table(table_dir, rows_by_partition, csv_schema, pq_schema=None,
                      partition_column="dt"):
    """Write the same logical table in both csv and parquet in one directory."""
    pq_schema = pq_schema or csv_schema
    for value, rows in sorted(rows_by_partition.items()):
        for backend_name, schema in (("csv", csv_schema), ("parquet", pq_schema)):
            backend = get_backend(backend_name)
            path = (
                Path(table_dir)
                / f"{partition_column}={value}"
                / f"part-00000.{backend.extension}"
            )
            backend.write_rows(path, rows, schema)


def _dual_roots(config, before_rows, after_rows, csv_schema=CROSS_SCHEMA,
                pq_schema=None, after_csv_schema=None, after_pq_schema=None):
    before = config.project_root / "dual" / "before" / "orders"
    after = config.project_root / "dual" / "after" / "orders"
    _write_dual_table(before, _group(before_rows), csv_schema, pq_schema)
    _write_dual_table(
        after, _group(after_rows), after_csv_schema or csv_schema,
        after_pq_schema or pq_schema or csv_schema,
    )
    return before, after


@requires_parquet
def test_normalise_value_covers_null_and_numeric_spellings():
    from lakeback.backfill.auditor import (
        normalise_schema,
        normalise_value,
        values_semantically_equal,
    )

    # Empty string and null are the same missing value.
    assert normalise_value("") is None
    assert normalise_value(None) is None
    # int/float drift collapses onto one numeric spelling.
    assert values_semantically_equal(1, 1.0)
    assert values_semantically_equal(42, 42.0)
    # Real changes must survive normalisation.
    assert not values_semantically_equal(1, 1.5)
    assert not values_semantically_equal("paid", "refunded")
    assert not values_semantically_equal("", "x")
    # bool must not be swallowed into the numeric family.
    assert not values_semantically_equal(True, 1)
    assert normalise_schema({"a": "int", "b": "float", "c": "null", "d": "str"}) == {
        "a": "num", "b": "num", "c": "null", "d": "str",
    }


@requires_parquet
def test_cross_backend_null_spelling_is_normalised_consistent(make_config):
    config = make_config()
    schema = {"order_id": "int", "note": "str", "dt": "str"}
    rows = [
        {"order_id": 1, "note": None, "dt": "2024-02-01"},
        {"order_id": 2, "note": "kept", "dt": "2024-02-01"},
    ]
    before, after = _dual_roots(config, rows, rows, csv_schema=schema)
    # The physical spellings really differ: csv persisted an empty cell where
    # parquet stored a null, which is the divergence the audit must reconcile.
    csv_text = (after / "dt=2024-02-01" / "part-00000.csv").read_text(encoding="utf-8")
    assert "1,," in csv_text
    report = run_cross_backend_audit(config, "orders", before, after)

    assert report["status"] == "consistent"
    assert report["backend_statuses"] == {"csv": "consistent", "parquet": "consistent"}
    assert report["cross_backend_conflicts"] == []
    assert report["audit_mode"] == "cross_backend"
    assert report["backend"] == "cross-backend"
    assert "note" in report["normalisation"]["fields_with_null_or_empty_values"]
    # The legacy contract fields are still present with their old structure.
    assert all(check["status"] == "pass" for check in report["checks"])
    assert report["counts"]["modified_rows"] == 0
    assert report["residual"]["modified_rows"] == []
    # Both backends hash to the same normalised fingerprint.
    fingerprints = set(report["normalised_fingerprints"].values())
    assert len(fingerprints) == 1
    markdown = (config.reports_dir / f"{report['report_id']}.md").read_text(encoding="utf-8")
    assert "Cross-Backend Equivalence" in markdown
    assert "No conflicts" in markdown


@requires_parquet
def test_cross_backend_null_to_value_change_is_not_washed_away(make_config):
    config = make_config()
    schema = {"order_id": "int", "note": "str", "dt": "str"}
    before_rows = [
        {"order_id": 1, "note": None, "dt": "2024-02-01"},
        {"order_id": 2, "note": "kept", "dt": "2024-02-01"},
    ]
    # A genuine business change: the missing value gets filled in.
    after_rows = [
        {"order_id": 1, "note": "filled-in", "dt": "2024-02-01"},
        {"order_id": 2, "note": "kept", "dt": "2024-02-01"},
    ]
    before, after = _dual_roots(config, before_rows, after_rows, csv_schema=schema)
    report = run_cross_backend_audit(config, "orders", before, after)

    assert report["status"] == "residual_diff"
    assert report["backend_statuses"] == {"csv": "residual_diff", "parquet": "residual_diff"}
    modified = report["residual"]["modified_rows"]
    assert len(modified) == 1 and modified[0]["key"] == 1
    note_change = next(f for f in modified[0]["fields"] if f["name"] == "note")
    assert note_change["normalised_before"] is None
    assert note_change["normalised_after"] == ("str", "filled-in")


@requires_parquet
def test_cross_backend_int_float_drift_is_normalised(make_config):
    config = make_config()
    int_schema = {"order_id": "int", "amount": "int", "dt": "str"}
    float_schema = {"order_id": "int", "amount": "float", "dt": "str"}
    rows = [
        {"order_id": 1, "amount": 10, "dt": "2024-02-01"},
        {"order_id": 2, "amount": 20, "dt": "2024-02-01"},
    ]
    # csv infers amount as int, parquet stores the same numbers as float.
    before, after = _dual_roots(
        config, rows, rows, csv_schema=int_schema, pq_schema=float_schema,
    )
    report = run_cross_backend_audit(config, "orders", before, after)

    assert report["status"] == "consistent"
    assert report["backend_statuses"] == {"csv": "consistent", "parquet": "consistent"}
    assert report["cross_backend_conflicts"] == []
    assert "amount" in report["normalisation"]["numeric_type_drift_fields"]

    # Without the cross-backend layer each backend's raw report is internally
    # consistent, but they describe the same data with different schema
    # spellings and therefore get different report ids: csv infers int,
    # parquet infers float for the identical numeric column.
    raw_csv = run_audit(config, "orders", before, after, backend_name="csv")
    raw_pq = run_audit(config, "orders", before, after, backend_name="parquet")
    assert raw_csv["status"] == "consistent"
    assert raw_pq["status"] == "consistent"
    csv_amount = next(
        item for item in raw_csv["checks"][0]["evidence"]["before_schema"]
        if item["name"] == "amount"
    )["type"]
    pq_amount = next(
        item for item in raw_pq["checks"][0]["evidence"]["before_schema"]
        if item["name"] == "amount"
    )["type"]
    assert (csv_amount, pq_amount) == ("int", "float")
    assert raw_csv["report_id"] != raw_pq["report_id"]
    # The reconciled cross-backend report gets its own stable semantic id.
    assert report["report_id"] not in {raw_csv["report_id"], raw_pq["report_id"]}


@requires_parquet
def test_cross_backend_normalises_before_after_int_float_drift(make_config, write_table):
    config = make_config()
    int_schema = {"order_id": "int", "amount": "int", "dt": "str"}
    float_schema = {"order_id": "int", "amount": "float", "dt": "str"}
    int_rows = [
        {"order_id": 1, "amount": 10, "dt": "2024-02-01"},
        {"order_id": 2, "amount": 20, "dt": "2024-02-01"},
    ]
    float_rows = [
        {"order_id": 1, "amount": 10.0, "dt": "2024-02-01"},
        {"order_id": 2, "amount": 20.0, "dt": "2024-02-01"},
    ]
    # Single-format directory pairs where before is int-typed and after is
    # float-typed; the raw audit calls this a structural mismatch even though
    # every numeric value is unchanged.
    csv_before = config.project_root / "drift" / "before" / "orders"
    csv_after = config.project_root / "drift" / "after" / "orders"
    write_table(csv_before, _group(int_rows), int_schema)
    write_table(csv_after, _group(float_rows), float_schema)
    raw = run_audit(config, "orders", csv_before, csv_after, backend_name="csv")
    assert raw["status"] == "structural_mismatch"

    # Add parquet copies of the same logical before/after and reconcile.
    pq = get_backend("parquet")
    for root, rows, schema in (
        (csv_before, int_rows, int_schema),
        (csv_after, float_rows, float_schema),
    ):
        pq.write_rows(
            root / "dt=2024-02-01" / "part-00000.parquet", rows, schema
        )
    report = run_cross_backend_audit(config, "orders", csv_before, csv_after)
    assert report["status"] == "consistent"
    assert report["backend_statuses"] == {"csv": "consistent", "parquet": "consistent"}
    assert report["cross_backend_conflicts"] == []
    assert "amount" in report["normalisation"]["numeric_type_drift_fields"]
    assert report["counts"]["modified_rows"] == 0


@requires_parquet
def test_cross_backend_schema_field_order_is_normalised(make_config):
    config = make_config()
    schema_one = {"order_id": "int", "amount": "float", "status": "str", "dt": "str"}
    schema_two = {"order_id": "int", "status": "str", "amount": "float", "dt": "str"}
    rows = [{"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"}]
    before, after = _dual_roots(
        config, rows, rows, csv_schema=schema_one,
        pq_schema=schema_one, after_csv_schema=schema_two, after_pq_schema=schema_two,
    )
    report = run_cross_backend_audit(config, "orders", before, after)
    assert report["status"] == "consistent"
    assert report["cross_backend_conflicts"] == []
    assert report["normalisation"]["field_order_reordered"] is True


@requires_parquet
def test_cross_backend_real_data_change_stays_residual_diff(make_config):
    config = make_config()
    before_rows = [
        {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
        {"order_id": 2, "amount": 20.0, "status": "paid", "dt": "2024-02-01"},
    ]
    after_rows = [
        {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
        {"order_id": 2, "amount": 25.5, "status": "refunded", "dt": "2024-02-01"},
    ]
    before, after = _dual_roots(config, before_rows, after_rows)
    report = run_cross_backend_audit(config, "orders", before, after)

    assert report["status"] == "residual_diff"
    assert report["backend_statuses"] == {"csv": "residual_diff", "parquet": "residual_diff"}
    assert report["cross_backend_conflicts"] == []
    modified = report["residual"]["modified_rows"]
    assert len(modified) == 1
    assert modified[0]["key"] == 2
    changed = {field["name"] for field in modified[0]["fields"]}
    assert changed == {"amount", "status"}
    # Normalisation must not have washed the changed values out.
    amount = next(f for f in modified[0]["fields"] if f["name"] == "amount")
    assert amount["normalised_before"] != amount["normalised_after"]
    assert report["partition_checksum_mismatches"]


@requires_parquet
def test_cross_backend_conflict_is_explicit_and_observable(make_config, monkeypatch):
    config = make_config()
    rows = [
        {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
        {"order_id": 2, "amount": 20.0, "status": "paid", "dt": "2024-02-01"},
    ]
    before, after = _dual_roots(config, rows, rows)

    # Simulate one backend reading the after directory with a missing row:
    # its normalised verdict must diverge from the other backend's.
    real_read = auditor_module.read_table_data

    def patched_read(backend, table_dir, *args, **kwargs):
        table = real_read(backend, table_dir, *args, **kwargs)
        if backend.name == "csv" and str(table_dir) == str(after):
            table.rows = [row for row in table.rows if row["order_id"] != 2]
            table.rows_by_partition = {
                marker: [row for row in part if row["order_id"] != 2]
                for marker, part in table.rows_by_partition.items()
            }
        return table

    monkeypatch.setattr(auditor_module, "read_table_data", patched_read)
    report = run_cross_backend_audit(config, "orders", before, after)

    assert report["status"] == CROSS_BACKEND_CONFLICT_STATUS
    assert report["backend_statuses"] == {"csv": "residual_diff", "parquet": "consistent"}
    check_names = {conflict["check"] for conflict in report["cross_backend_conflicts"]}
    assert {"row_count", "primary_key_set", "partition_checksums"} <= check_names
    key_conflict = next(
        conflict for conflict in report["cross_backend_conflicts"]
        if conflict["check"] == "primary_key_set"
    )
    assert key_conflict["keys"]["primary_key"] == "order_id"
    assert key_conflict["keys"]["conflicting_missing_keys"] == {"csv": [2]}
    checksum_conflict = next(
        conflict for conflict in report["cross_backend_conflicts"]
        if conflict["check"] == "partition_checksums"
    )
    assert checksum_conflict["partitions"]["csv"][0]["partition"] == {"dt": "2024-02-01"}
    # The conflict is visible in both persisted artifacts.
    markdown = (config.reports_dir / f"{report['report_id']}.md").read_text(encoding="utf-8")
    assert "Cross-Backend Conflicts" in markdown
    assert "primary_key_set" in markdown
    loaded = load_report(config.reports_dir, report["report_id"])
    assert loaded["status"] == CROSS_BACKEND_CONFLICT_STATUS
    assert loaded["cross_backend_conflicts"] == report["cross_backend_conflicts"]


@requires_parquet
def test_cross_backend_report_id_is_stable_across_backends(make_config):
    config = make_config()
    rows = [
        {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
        {"order_id": 2, "amount": 20.0, "status": "paid", "dt": "2024-02-02"},
    ]
    before, after = _dual_roots(config, rows, rows)

    first = run_cross_backend_audit(config, "orders", before, after)
    second = run_cross_backend_audit(config, "orders", before, after)
    assert first["report_id"] == second["report_id"]
    assert REPORT_RE.fullmatch(first["report_id"])
    # The same logical content rebuilt in a fresh dual-format directory pair
    # produces the same id even though the physical files differ.
    other_before = config.project_root / "dual-copy" / "before" / "orders"
    other_after = config.project_root / "dual-copy" / "after" / "orders"
    _write_dual_table(other_before, _group(rows), CROSS_SCHEMA)
    _write_dual_table(other_after, _group(rows), CROSS_SCHEMA)
    rebuilt = run_cross_backend_audit(config, "orders", other_before, other_after)
    assert rebuilt["report_id"] == first["report_id"]

    # Different logical content must hash differently.
    changed = [{**row, "status": "refunded"} if row["order_id"] == 1 else row
               for row in rows]
    changed_after = config.project_root / "dual-changed" / "after" / "orders"
    changed_before = config.project_root / "dual-changed" / "before" / "orders"
    _write_dual_table(changed_before, _group(rows), CROSS_SCHEMA)
    _write_dual_table(changed_after, _group(changed), CROSS_SCHEMA)
    different = run_cross_backend_audit(
        config, "orders", changed_before, changed_after
    )
    assert different["report_id"] != first["report_id"]


@requires_parquet
def test_cross_backend_keeps_partition_level_conflicts(make_config, monkeypatch):
    config = make_config()
    rows = [
        {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
        {"order_id": 2, "amount": 20.0, "status": "paid", "dt": "2024-02-02"},
    ]
    before, after = _dual_roots(config, rows, rows)

    real_read = auditor_module.read_table_data

    def patched_read(backend, table_dir, *args, **kwargs):
        table = real_read(backend, table_dir, *args, **kwargs)
        if backend.name == "parquet" and str(table_dir) == str(after):
            # Wipe one partition on one backend only; the partition marker
            # still exists but its checksum no longer matches.
            target = next(
                marker for marker in table.rows_by_partition
                if dict(marker) == {"dt": "2024-02-02"}
            )
            table.rows_by_partition = dict(table.rows_by_partition)
            table.rows_by_partition[target] = []
            table.rows = [
                row for row in table.rows
                if row["dt"] != "2024-02-02"
            ]
        return table

    monkeypatch.setattr(auditor_module, "read_table_data", patched_read)
    report = run_cross_backend_audit(config, "orders", before, after)
    assert report["status"] == CROSS_BACKEND_CONFLICT_STATUS
    partition_conflict = next(
        conflict for conflict in report["cross_backend_conflicts"]
        if conflict["check"] == "partition_checksums"
    )
    assert partition_conflict["partitions"]["parquet"][0]["partition"] == {
        "dt": "2024-02-02"
    }


def test_cross_backend_requires_existing_roots(make_config):
    config = make_config()
    with pytest.raises(NotFoundError, match="before table directory"):
        run_cross_backend_audit(
            config, "orders",
            config.project_root / "missing", config.project_root,
        )
