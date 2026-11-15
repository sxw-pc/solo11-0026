"""Compare two local table directories and explain every consistency decision."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from lakeback._util import NAME_RE
from lakeback.backfill.compare import (
    checksum_evidence,
    compare_keys,
    fingerprint,
    partition_evidence,
    partition_payload,
    schema_evidence,
)
from lakeback.backfill.expectations import ExpectedChanges, load_expected_changes
from lakeback.backfill.report import write_audit_report
from lakeback.config import Config
from lakeback.errors import ConfigError, DataError, NotFoundError
from lakeback.tables import read_table_data

SUMMARY = {
    "consistent": "all structural, count, key and checksum checks passed",
    "residual_diff": "structure matches but row or checksum differences remain",
    "structural_mismatch": "schema or partition structure differs",
}


def _check(name: str, passed: bool, evidence: dict) -> dict:
    return {"name": name, "status": "pass" if passed else "fail", "evidence": evidence}


def _resolve_roots(
    before_root: Path | str, after_root: Path | str
) -> tuple[Path, Path]:
    before_path = Path(before_root).resolve()
    after_path = Path(after_root).resolve()
    if not before_path.is_dir():
        raise NotFoundError(f"before table directory does not exist: {before_path}")
    if not after_path.is_dir():
        raise NotFoundError(f"after table directory does not exist: {after_path}")
    return before_path, after_path


def _expected_block(
    table: str,
    expectations: ExpectedChanges,
    expected_rows: list[dict],
    expected_partition_mismatches: list[dict],
    expected_field_count: int,
    expected_partition_fields: int,
) -> dict:
    return {
        "declaration": expectations.metadata(table),
        "modified_rows": expected_rows,
        "partition_fields": expected_partition_mismatches,
        "counts": {
            "modified_rows": len(expected_rows),
            "fields": expected_field_count,
            "partition_fields": expected_partition_fields,
            "partition_count": len(expected_partition_mismatches),
        },
    }


def run_audit(
    config: Config,
    table: str,
    before_root: Path | str,
    after_root: Path | str,
    *,
    key: str | None = None,
    backend_name: str | None = None,
    clock: Callable[[], str] | None = None,
    expected_changes: ExpectedChanges | dict | list | str | Path | None = None,
) -> dict:
    """Run the audit, write JSON/Markdown artifacts, and return the full report.

    ``expected_changes`` may be a declaration path, a parsed document, or an
    ``ExpectedChanges`` instance. Declared field changes are reported as
    expected and no longer force ``residual_diff``; everything else keeps the
    original residual semantics. The declaration content participates in the
    ``report_id`` fingerprint.
    """
    if not NAME_RE.fullmatch(table):
        raise DataError(f"invalid table name: {table!r}")
    expectations = load_expected_changes(expected_changes)
    backend = config.resolve_backend(backend_name)
    before_path, after_path = _resolve_roots(before_root, after_root)
    before = read_table_data(backend, before_path)
    after = read_table_data(backend, after_path)
    primary_key = key or config.table_key(table)
    if primary_key not in before.schema or primary_key not in after.schema:
        raise ConfigError(
            f"primary key {primary_key!r} is not present in both table schemas",
            details={
                "before_has_key": primary_key in before.schema,
                "after_has_key": primary_key in after.schema,
            },
        )
    if expectations is not None:
        expectations.validate_table(table, before.schema, after.schema, primary_key)

    schema_ok, schema_evidence_block = schema_evidence(before.schema, after.schema)
    partition_ok, partition_evidence_block, missing_parts, extra_parts = (
        partition_evidence(before, after)
    )
    row_count_ok = before.row_count == after.row_count
    key_evidence, missing_keys, extra_keys, modified, unchanged = compare_keys(
        before, after, primary_key
    )
    key_ok = not missing_keys and not extra_keys
    expected_rows, residual_rows = (
        expectations.split_modified_rows(table, modified)
        if expectations is not None
        else ([], modified)
    )
    expected_field_count = sum(len(item["fields"]) for item in expected_rows)
    common = sorted(set(before.rows_by_partition) & set(after.rows_by_partition))
    (
        checksum_ok, checksum_details, checksum_mismatches,
        expected_partition_mismatches, expected_partition_fields,
    ) = checksum_evidence(before, after, primary_key, common, table, expectations)
    checks = [
        _check("schema", schema_ok, schema_evidence_block),
        _check("partition_structure", partition_ok, partition_evidence_block),
        _check("row_count", row_count_ok, {
            "before_rows": before.row_count, "after_rows": after.row_count,
            "delta": after.row_count - before.row_count,
        }),
        _check("primary_key_set", key_ok, key_evidence),
        _check("partition_checksums", checksum_ok, {
            "common_partitions": len(common),
            "partitions": checksum_details,
        }),
    ]
    structural = not schema_ok or not partition_ok
    residual = (
        not row_count_ok or not key_ok or not checksum_ok
        or bool(missing_keys) or bool(extra_keys) or bool(residual_rows)
    )
    if structural:
        status = "structural_mismatch"
    elif residual:
        status = "residual_diff"
    else:
        status = "consistent"

    report = {
        "format": 1,
        "report_id": f"rpt-{fingerprint(table, primary_key, before, after, expectations)}",
        "table": table,
        "key": primary_key,
        "status": status,
        "generated_at": clock() if clock else config.now_iso(),
        "backend": backend.name,
        "before_root": str(before_path),
        "after_root": str(after_path),
        "checks": checks,
        "counts": {
            "before_rows": before.row_count,
            "after_rows": after.row_count,
            "before_keys": key_evidence["before_keys"],
            "after_keys": key_evidence["after_keys"],
            "missing_in_after": len(missing_keys),
            "extra_in_after": len(extra_keys),
            "modified_rows": len(modified),
            "unchanged_rows": unchanged,
            "expected_fields": expected_field_count,
            "residual_modified_rows": len(residual_rows),
        },
        "residual": {
            "missing_in_after": missing_keys,
            "extra_in_after": extra_keys,
            "modified_rows": residual_rows,
            "missing_partitions": [partition_payload(item) for item in missing_parts],
            "extra_partitions": [partition_payload(item) for item in extra_parts],
        },
        "partition_checksum_mismatches": checksum_mismatches,
    }
    if expectations is not None:
        report["expected_changes"] = _expected_block(
            table, expectations, expected_rows, expected_partition_mismatches,
            expected_field_count, expected_partition_fields,
        )
    report["summary"] = SUMMARY[status]
    if status == "consistent" and expected_field_count:
        report["summary"] = (
            f"all residual checks passed; {expected_field_count} declared "
            "field change(s) were within expectation"
        )
    json_path, markdown_path = write_audit_report(config.reports_dir, report)
    report["artifacts"] = {
        "json_report": str(json_path),
        "markdown_report": str(markdown_path),
    }
    return report
