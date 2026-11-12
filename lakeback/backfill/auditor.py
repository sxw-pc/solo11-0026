"""Compare two local table directories and explain every consistency decision."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from lakeback._util import (
    NAME_RE,
    canonical_json,
    ensure_unique,
    primary_key_sort,
    sha256_text,
    stable_rows,
)
from lakeback.backfill.report import write_audit_report
from lakeback.config import Config
from lakeback.errors import ConfigError, DataError, NotFoundError
from lakeback.snapshot.manifest import schema_as_list
from lakeback.tables import TableData, read_table_data


def _check(name: str, passed: bool, evidence: dict) -> dict:
    return {"name": name, "status": "pass" if passed else "fail", "evidence": evidence}


def _partition_payload(marker: tuple) -> dict:
    return dict(marker)


def _schema_evidence(before: dict, after: dict) -> tuple[bool, dict]:
    differences = {
        "missing_fields": sorted(set(before) - set(after)),
        "extra_fields": sorted(set(after) - set(before)),
        "type_changes": {
            name: {"before": before[name], "after": after[name]}
            for name in sorted(set(before) & set(after))
            if before[name] != after[name]
        },
        "order_changed": list(before) != list(after),
    }
    passed = not any([
        differences["missing_fields"], differences["extra_fields"],
        differences["type_changes"], differences["order_changed"],
    ])
    return passed, {
        "before_schema": schema_as_list(before),
        "after_schema": schema_as_list(after),
        "differences": differences,
    }


def _partition_evidence(before: TableData, after: TableData) -> tuple[bool, dict, list, list]:
    before_set = set(before.rows_by_partition)
    after_set = set(after.rows_by_partition)
    missing = sorted(before_set - after_set)
    extra = sorted(after_set - before_set)
    passed = before.partition_columns == after.partition_columns and not missing and not extra
    evidence = {
        "before_columns": before.partition_columns,
        "after_columns": after.partition_columns,
        "before_partitions": [_partition_payload(item) for item in sorted(before_set)],
        "after_partitions": [_partition_payload(item) for item in sorted(after_set)],
        "missing_in_after": [_partition_payload(item) for item in missing],
        "extra_in_after": [_partition_payload(item) for item in extra],
    }
    return passed, evidence, missing, extra


def _keyed(rows: list[dict], key: str) -> dict[str, dict]:
    try:
        ensure_unique(rows, key)
    except ValueError as exc:
        raise DataError(str(exc)) from exc
    return {canonical_json(row[key]): row for row in rows}


def _field_checksums(rows: list[dict], schema: dict, key: str) -> tuple[dict[str, str], str]:
    ordered = sorted(rows, key=lambda row: primary_key_sort(row[key]))
    fields = {
        name: sha256_text(canonical_json([row.get(name) for row in ordered]))
        for name in schema
    }
    return fields, sha256_text(canonical_json(fields))


def _compare_keys(
    before: TableData, after: TableData, key: str
) -> tuple[dict, list, list, list, int]:
    left = _keyed(before.rows, key)
    right = _keyed(after.rows, key)
    left_only = [left[marker][key] for marker in left.keys() - right.keys()]
    right_only = [right[marker][key] for marker in right.keys() - left.keys()]
    left_only.sort(key=primary_key_sort)
    right_only.sort(key=primary_key_sort)
    shared = sorted(left.keys() & right.keys(), key=lambda marker: primary_key_sort(left[marker][key]))
    field_order = list(before.schema)
    field_order.extend(name for name in after.schema if name not in before.schema)
    modified: list[dict] = []
    for marker in shared:
        changes = [
            {"name": name, "before": left[marker].get(name), "after": right[marker].get(name)}
            for name in field_order
            if left[marker].get(name) != right[marker].get(name)
        ]
        if changes:
            modified.append({"key": left[marker][key], "fields": changes})
    unchanged = len(shared) - len(modified)
    key_evidence = {
        "before_keys": len(left),
        "after_keys": len(right),
        "missing_in_after": left_only,
        "extra_in_after": right_only,
    }
    return key_evidence, left_only, right_only, modified, unchanged


def _checksum_evidence(
    before: TableData, after: TableData, key: str, common: list[tuple]
) -> tuple[bool, list, list]:
    details: list[dict] = []
    mismatches: list[dict] = []
    for marker in common:
        before_fields, before_digest = _field_checksums(
            before.rows_by_partition[marker], before.schema, key
        )
        after_fields, after_digest = _field_checksums(
            after.rows_by_partition[marker], after.schema, key
        )
        mismatched = sorted(
            name for name in set(before_fields) | set(after_fields)
            if before_fields.get(name) != after_fields.get(name)
        )
        partition = _partition_payload(marker)
        details.append({
            "partition": partition,
            "before_digest": before_digest,
            "after_digest": after_digest,
            "mismatched_fields": mismatched,
        })
        if mismatched:
            mismatches.append({"partition": partition, "mismatched_fields": mismatched})
    return not mismatches, details, mismatches


def _fingerprint(table: str, key: str, before: TableData, after: TableData) -> str:
    payload = {
        "table": table,
        "key": key,
        "before": {
            "schema": schema_as_list(before.schema),
            "partitions": before.partitions,
            "rows": stable_rows(before.rows, key),
        },
        "after": {
            "schema": schema_as_list(after.schema),
            "partitions": after.partitions,
            "rows": stable_rows(after.rows, key),
        },
    }
    return sha256_text(canonical_json(payload))[:16]


def run_audit(
    config: Config,
    table: str,
    before_root: Path | str,
    after_root: Path | str,
    *,
    key: str | None = None,
    backend_name: str | None = None,
    clock: Callable[[], str] | None = None,
) -> dict:
    """Run the audit, write JSON/Markdown artifacts, and return the full report."""
    if not NAME_RE.fullmatch(table):
        raise DataError(f"invalid table name: {table!r}")
    backend = config.resolve_backend(backend_name)
    before_path = Path(before_root).resolve()
    after_path = Path(after_root).resolve()
    if not before_path.is_dir():
        raise NotFoundError(f"before table directory does not exist: {before_path}")
    if not after_path.is_dir():
        raise NotFoundError(f"after table directory does not exist: {after_path}")
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

    schema_ok, schema_evidence = _schema_evidence(before.schema, after.schema)
    partition_ok, partition_evidence, missing_parts, extra_parts = _partition_evidence(
        before, after
    )
    row_count_ok = before.row_count == after.row_count
    key_evidence, missing_keys, extra_keys, modified, unchanged = _compare_keys(
        before, after, primary_key
    )
    key_ok = not missing_keys and not extra_keys
    common = sorted(set(before.rows_by_partition) & set(after.rows_by_partition))
    checksum_ok, checksum_details, checksum_mismatches = _checksum_evidence(
        before, after, primary_key, common
    )
    checks = [
        _check("schema", schema_ok, schema_evidence),
        _check("partition_structure", partition_ok, partition_evidence),
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
        or bool(missing_keys) or bool(extra_keys) or bool(modified)
    )
    if structural:
        status = "structural_mismatch"
    elif residual:
        status = "residual_diff"
    else:
        status = "consistent"

    report = {
        "format": 1,
        "report_id": f"rpt-{_fingerprint(table, primary_key, before, after)}",
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
        },
        "residual": {
            "missing_in_after": missing_keys,
            "extra_in_after": extra_keys,
            "modified_rows": modified,
            "missing_partitions": [_partition_payload(item) for item in missing_parts],
            "extra_partitions": [_partition_payload(item) for item in extra_parts],
        },
        "partition_checksum_mismatches": checksum_mismatches,
    }
    report["summary"] = {
        "consistent": "all structural, count, key and checksum checks passed",
        "residual_diff": "structure matches but row or checksum differences remain",
        "structural_mismatch": "schema or partition structure differs",
    }[status]
    json_path, markdown_path = write_audit_report(config.reports_dir, report)
    report["artifacts"] = {
        "json_report": str(json_path),
        "markdown_report": str(markdown_path),
    }
    return report
