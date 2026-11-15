"""Pure comparison helpers behind the backfill audit checks."""

from __future__ import annotations

from lakeback._util import (
    canonical_json,
    ensure_unique,
    primary_key_sort,
    sha256_text,
    stable_rows,
)
from lakeback.backfill.expectations import ExpectedChanges
from lakeback.errors import DataError
from lakeback.snapshot.manifest import schema_as_list
from lakeback.tables import TableData


def partition_payload(marker: tuple) -> dict:
    return dict(marker)


def schema_evidence(before: dict, after: dict) -> tuple[bool, dict]:
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


def partition_evidence(before: TableData, after: TableData) -> tuple[bool, dict, list, list]:
    before_set = set(before.rows_by_partition)
    after_set = set(after.rows_by_partition)
    missing = sorted(before_set - after_set)
    extra = sorted(after_set - before_set)
    passed = before.partition_columns == after.partition_columns and not missing and not extra
    evidence = {
        "before_columns": before.partition_columns,
        "after_columns": after.partition_columns,
        "before_partitions": [partition_payload(item) for item in sorted(before_set)],
        "after_partitions": [partition_payload(item) for item in sorted(after_set)],
        "missing_in_after": [partition_payload(item) for item in missing],
        "extra_in_after": [partition_payload(item) for item in extra],
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


def compare_keys(
    before: TableData, after: TableData, key: str
) -> tuple[dict, list, list, list, int]:
    left = _keyed(before.rows, key)
    right = _keyed(after.rows, key)
    left_only = [left[marker][key] for marker in left.keys() - right.keys()]
    right_only = [right[marker][key] for marker in right.keys() - left.keys()]
    left_only.sort(key=primary_key_sort)
    right_only.sort(key=primary_key_sort)
    shared = sorted(
        left.keys() & right.keys(), key=lambda marker: primary_key_sort(left[marker][key])
    )
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


def checksum_evidence(
    before: TableData,
    after: TableData,
    key: str,
    common: list[tuple],
    table: str,
    expectations: ExpectedChanges | None,
) -> tuple[bool, list, list, list, int]:
    """Compare per-field partition checksums, honouring declared fields.

    Each partition records both the fields whose value drift is fully
    declared and the fields that remain residual; ``checksum_ok`` only
    ignores the former.
    """
    details: list[dict] = []
    mismatches: list[dict] = []
    expected_mismatches: list[dict] = []
    expected_field_count = 0
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
        expected_fields: list[str] = []
        residual_fields = list(mismatched)
        if expectations is not None and mismatched:
            expected_fields, residual_fields = expectations.explain_partition_fields(
                table,
                before.rows_by_partition[marker],
                after.rows_by_partition[marker],
                key,
                mismatched,
            )
        partition = partition_payload(marker)
        details.append({
            "partition": partition,
            "before_digest": before_digest,
            "after_digest": after_digest,
            "mismatched_fields": mismatched,
            "expected_fields": expected_fields,
            "residual_fields": residual_fields,
        })
        if mismatched:
            mismatches.append({
                "partition": partition,
                "mismatched_fields": mismatched,
                "expected_fields": expected_fields,
                "residual_fields": residual_fields,
            })
        if expected_fields:
            expected_field_count += len(expected_fields)
            expected_mismatches.append({
                "partition": partition,
                "expected_fields": expected_fields,
            })
    checksum_ok = not any(item["residual_fields"] for item in mismatches)
    return (
        checksum_ok, details, mismatches, expected_mismatches, expected_field_count,
    )


def fingerprint(
    table: str,
    key: str,
    before: TableData,
    after: TableData,
    expectations: ExpectedChanges | None = None,
) -> str:
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
    if expectations is not None:
        payload["expected_changes"] = expectations.fingerprint_payload()
    return sha256_text(canonical_json(payload))[:16]
