"""Compare two local table directories and explain every consistency decision.

The module offers two audit entry points:

* :func:`run_audit` keeps the original single-backend contract: one backend
  reads both directories and the raw backend spelling (string vs null, int vs
  float, schema field order) drives every verdict.
* :func:`run_cross_backend_audit` reads the same before/after directories once
  per backend, normalises backend-specific spellings into one shared semantic
  form, and only then compares the per-backend verdicts.  Diverging verdicts
  become an explicit ``cross_backend_conflict`` status instead of being silently
  collapsed onto one backend's answer.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

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

# Backends that participate in cross-backend equivalence audits.
CROSS_BACKEND_AUDIT_BACKENDS = ("csv", "parquet")

# Final status used when the normalised per-backend verdicts disagree.
CROSS_BACKEND_CONFLICT_STATUS = "cross_backend_conflict"


def _check(name: str, passed: bool, evidence: dict) -> dict:
    return {"name": name, "status": "pass" if passed else "fail", "evidence": evidence}


def _partition_payload(marker: tuple) -> dict:
    return dict(marker)


# ---------------------------------------------------------------------------
# Semantic normalisation
#
# These helpers erase only the spelling differences that are *known* to be
# backend artefacts.  A genuinely changed value must still compare unequal:
#   * None <-> ""              -> both mean "no value" for a table cell
#   * 1 <-> 1.0                -> same numeric value, int/float type drift
#   * schema field order       -> sets of (name, type family) are compared
#   * schema "null" <-> "str"  -> a null-only column infers differently
# Everything else (a real string change, a different number, a missing key,
# an extra partition, a bool/int mismatch, ...) survives normalisation.
# ---------------------------------------------------------------------------

def normalise_value(value: Any) -> Any:
    """Map one cell value onto its backend-independent semantic spelling."""
    if value is None:
        return None
    if isinstance(value, str) and value == "":
        # CSV persists nulls as empty cells; treat the two as the same value.
        return None
    if isinstance(value, bool):
        # bool must stay distinct from int: True == 1 in Python, but not in data.
        return ("bool", value)
    if isinstance(value, int):
        return ("num", float(value))
    if isinstance(value, float):
        return ("num", value)
    return ("str", value)


def values_semantically_equal(left: Any, right: Any) -> bool:
    """Whether two cell values carry the same business meaning."""
    return normalise_value(left) == normalise_value(right)


def normalise_type(kind: str) -> str:
    """Collapse portable schema types onto semantic type families."""
    if kind in {"int", "float"}:
        return "num"
    if kind == "null":
        # A column containing only nulls is inferred as null (parquet) or
        # str (csv); the null family is compatible with the str family.
        return "null"
    return kind


def normalise_schema(schema: Mapping[str, str]) -> dict[str, str]:
    """Return the order-independent semantic schema used for comparisons."""
    return {name: normalise_type(kind) for name, kind in schema.items()}


def schemas_semantically_equal(left: Mapping[str, str], right: Mapping[str, str]) -> bool:
    """Field sets and type families must match; field order is irrelevant."""
    return normalise_schema(left) == normalise_schema(right)


def _normalised_row(row: Mapping[str, Any], fields: list[str]) -> dict[str, Any]:
    return {name: normalise_value(row.get(name)) for name in fields}


def _normalised_rows(rows: list[dict], fields: list[str], key: str) -> list[dict]:
    ordered = sorted(rows, key=lambda row: primary_key_sort(row[key]))
    return [_normalised_row(row, fields) for row in ordered]


def _semantic_key_marker(value: Any) -> str:
    return canonical_json(normalise_value(value))


# ---------------------------------------------------------------------------
# Original single-backend audit (contract preserved verbatim in behaviour)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Normalised per-backend audit
# ---------------------------------------------------------------------------

def _normalised_schema_evidence(before: TableData, after: TableData) -> tuple[bool, dict]:
    left = normalise_schema(before.schema)
    right = normalise_schema(after.schema)
    missing_fields = sorted(set(left) - set(right))
    extra_fields = sorted(set(right) - set(left))
    type_family_changes = {
        name: {"before": left[name], "after": right[name]}
        for name in sorted(set(left) & set(right))
        if left[name] != right[name]
    }
    # "null" and "str" are the same family for a null-only column: csv infers
    # str for an empty column, parquet keeps the arrow null type.
    compatible_nullability = {
        name: change
        for name, change in type_family_changes.items()
        if set(change.values()) <= {"null", "str"}
    }
    for name in compatible_nullability:
        type_family_changes.pop(name)
    order_changed = list(before.schema) != list(after.schema)
    numeric_drift = sorted(
        name for name in set(before.schema) & set(after.schema)
        if {before.schema[name], after.schema[name]} <= {"int", "float"}
        and before.schema[name] != after.schema[name]
    )
    null_spelling = sorted(
        name for name in set(before.schema) & set(after.schema)
        if {before.schema[name], after.schema[name]} <= {"null", "str"}
        and before.schema[name] != after.schema[name]
    )
    differences = {
        "missing_fields": missing_fields,
        "extra_fields": extra_fields,
        "type_family_changes": type_family_changes,
        "order_changed": order_changed,
        "normalised": {
            "numeric_type_drift_fields": numeric_drift,
            "null_type_spelling_fields": null_spelling,
            "field_order_reordered": order_changed,
        },
    }
    passed = not missing_fields and not extra_fields and not type_family_changes
    return passed, {
        "before_schema": schema_as_list(before.schema),
        "after_schema": schema_as_list(after.schema),
        "normalised_before_schema": [
            {"name": name, "type": left[name]} for name in sorted(left)
        ],
        "normalised_after_schema": [
            {"name": name, "type": right[name]} for name in sorted(right)
        ],
        "differences": differences,
    }


def _normalised_keyed(rows: list[dict], key: str) -> dict[str, dict]:
    markers: dict[str, dict] = {}
    for row in rows:
        if row.get(key) is None:
            raise DataError(f"row is missing primary key {key!r}")
        marker = _semantic_key_marker(row[key])
        if marker in markers:
            raise DataError(
                f"duplicate primary key {key}={row[key]!r} after normalisation"
            )
        markers[marker] = row
    return markers


def _normalised_compare_keys(
    before: TableData, after: TableData, key: str, fields: list[str]
) -> tuple[dict, list, list, list, int]:
    left = _normalised_keyed(before.rows, key)
    right = _normalised_keyed(after.rows, key)
    left_only = sorted(
        (left[marker][key] for marker in left.keys() - right.keys()),
        key=primary_key_sort,
    )
    right_only = sorted(
        (right[marker][key] for marker in right.keys() - left.keys()),
        key=primary_key_sort,
    )
    shared = sorted(
        left.keys() & right.keys(),
        key=lambda marker: primary_key_sort(left[marker][key]),
    )
    modified: list[dict] = []
    for marker in shared:
        left_row = _normalised_row(left[marker], fields)
        right_row = _normalised_row(right[marker], fields)
        changes = [
            {
                "name": name,
                "before": left[marker].get(name),
                "after": right[marker].get(name),
                "normalised_before": left_row[name],
                "normalised_after": right_row[name],
            }
            for name in fields
            if left_row[name] != right_row[name]
        ]
        if changes:
            modified.append({"key": left[marker][key], "fields": changes})
    unchanged = len(shared) - len(modified)
    evidence = {
        "before_keys": len(left),
        "after_keys": len(right),
        "missing_in_after": left_only,
        "extra_in_after": right_only,
    }
    return evidence, left_only, right_only, modified, unchanged


def _normalised_field_checksum(
    rows: list[dict], fields: list[str], key: str
) -> tuple[dict[str, str], str]:
    ordered = sorted(rows, key=lambda row: primary_key_sort(row[key]))
    per_field = {
        name: sha256_text(
            canonical_json([normalise_value(row.get(name)) for row in ordered])
        )
        for name in fields
    }
    return per_field, sha256_text(canonical_json(per_field))


def _normalised_checksum_evidence(
    before: TableData, after: TableData, key: str, fields: list[str],
    common: list[tuple],
) -> tuple[bool, list, list]:
    details: list[dict] = []
    mismatches: list[dict] = []
    for marker in common:
        before_fields, before_digest = _normalised_field_checksum(
            before.rows_by_partition[marker], fields, key
        )
        after_fields, after_digest = _normalised_field_checksum(
            after.rows_by_partition[marker], fields, key
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


def _normalised_fingerprint(
    table: str, key: str, before: TableData, after: TableData, fields: list[str]
) -> str:
    """Content hash over semantically normalised data, stable across backends."""
    payload = {
        "table": table,
        "key": key,
        "fields": fields,
        "before": {
            "schema": [
                {"name": name, "type": normalise_schema(before.schema)[name]}
                for name in fields
            ],
            "partition_columns": before.partition_columns,
            "partitions": [_partition_payload(item) for item in sorted(
                set(before.rows_by_partition)
            )],
            "rows": _normalised_rows(before.rows, fields, key),
        },
        "after": {
            "schema": [
                {"name": name, "type": normalise_schema(after.schema)[name]}
                for name in fields
            ],
            "partition_columns": after.partition_columns,
            "partitions": [_partition_payload(item) for item in sorted(
                set(after.rows_by_partition)
            )],
            "rows": _normalised_rows(after.rows, fields, key),
        },
    }
    return sha256_text(canonical_json(payload))[:16]


def _normalised_audit_once(
    table: str, primary_key: str, before: TableData, after: TableData
) -> dict:
    """Run every check on one backend's data using semantic normalisation."""
    fields = sorted(set(before.schema) | set(after.schema))

    schema_ok, schema_evidence = _normalised_schema_evidence(before, after)
    partition_ok, partition_evidence, missing_parts, extra_parts = _partition_evidence(
        before, after
    )
    row_count_ok = before.row_count == after.row_count
    key_evidence, missing_keys, extra_keys, modified, unchanged = _normalised_compare_keys(
        before, after, primary_key, fields
    )
    key_ok = not missing_keys and not extra_keys
    common = sorted(set(before.rows_by_partition) & set(after.rows_by_partition))
    checksum_ok, checksum_details, checksum_mismatches = _normalised_checksum_evidence(
        before, after, primary_key, fields, common
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
    return {
        "status": status,
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
        "fingerprint": _normalised_fingerprint(
            table, primary_key, before, after, fields
        ),
        "schema_evidence": schema_evidence,
        "fields": fields,
        "before": before,
        "after": after,
    }


# ---------------------------------------------------------------------------
# Cross-backend equivalence audit
# ---------------------------------------------------------------------------

def _check_conflicts(views: dict[str, dict], primary_key: str) -> list[dict]:
    """Describe every check whose normalised verdict differs across backends."""
    backend_names = sorted(views)
    reference = views[backend_names[0]]
    conflicts: list[dict] = []
    reference_checks = {check["name"]: check for check in reference["checks"]}
    for name in backend_names[1:]:
        candidate_checks = {check["name"]: check for check in views[name]["checks"]}
        for check_name in reference_checks:
            left_check = reference_checks[check_name]
            right_check = candidate_checks[check_name]
            if left_check["status"] == right_check["status"]:
                continue
            detail: dict = {
                "check": check_name,
                "verdicts": {
                    backend_names[0]: left_check["status"],
                    name: right_check["status"],
                },
            }
            if check_name == "primary_key_set":
                detail["keys"] = _key_conflict_detail(views, primary_key)
            elif check_name == "partition_structure":
                detail["partitions"] = _partition_conflict_detail(views)
            elif check_name == "partition_checksums":
                detail["partitions"] = _checksum_conflict_detail(views)
            elif check_name == "schema":
                detail["schema"] = _schema_conflict_detail(views)
            elif check_name == "row_count":
                detail["row_counts"] = {
                    backend: {
                        "before_rows": view["counts"]["before_rows"],
                        "after_rows": view["counts"]["after_rows"],
                    }
                    for backend, view in views.items()
                }
            conflicts.append(detail)
    return conflicts


def _key_conflict_detail(views: dict[str, dict], primary_key: str) -> dict:
    per_backend: dict[str, dict] = {}
    for backend, view in views.items():
        residual = view["residual"]
        per_backend[backend] = {
            "missing_in_after": residual["missing_in_after"],
            "extra_in_after": residual["extra_in_after"],
            "modified_keys": [item["key"] for item in residual["modified_rows"]],
        }
    missing = {
        backend: payload["missing_in_after"]
        for backend, payload in per_backend.items() if payload["missing_in_after"]
    }
    extra = {
        backend: payload["extra_in_after"]
        for backend, payload in per_backend.items() if payload["extra_in_after"]
    }
    modified = {
        backend: payload["modified_keys"]
        for backend, payload in per_backend.items() if payload["modified_keys"]
    }
    return {
        "primary_key": primary_key,
        "per_backend": per_backend,
        "conflicting_missing_keys": missing,
        "conflicting_extra_keys": extra,
        "conflicting_modified_keys": modified,
    }


def _partition_conflict_detail(views: dict[str, dict]) -> dict:
    per_backend: dict[str, dict] = {}
    for backend, view in views.items():
        residual = view["residual"]
        per_backend[backend] = {
            "missing_partitions": residual["missing_partitions"],
            "extra_partitions": residual["extra_partitions"],
        }
    return {
        "per_backend": per_backend,
        "conflicting_missing_partitions": {
            backend: payload["missing_partitions"]
            for backend, payload in per_backend.items()
            if payload["missing_partitions"]
        },
        "conflicting_extra_partitions": {
            backend: payload["extra_partitions"]
            for backend, payload in per_backend.items()
            if payload["extra_partitions"]
        },
    }


def _checksum_conflict_detail(views: dict[str, dict]) -> dict:
    return {
        backend: view["partition_checksum_mismatches"]
        for backend, view in views.items()
        if view["partition_checksum_mismatches"]
    }


def _schema_conflict_detail(views: dict[str, dict]) -> dict:
    return {
        backend: view["schema_evidence"]["differences"]
        for backend, view in views.items()
    }


def _normalisation_notes(views: dict[str, dict]) -> dict:
    """Summarise which backend artefacts were erased while normalising."""
    numeric_drift: set[str] = set()
    null_spelling: set[str] = set()
    reordered = False
    null_value_fields: set[str] = set()
    # Remember the raw schema each backend inferred for each side, so drift
    # *between backends* (the same column read as int by csv, float by
    # parquet) is reported as well as drift between before and after.
    raw_schemas: dict[str, list[dict[str, str]]] = {}
    for backend, view in views.items():
        differences = view["schema_evidence"]["differences"]["normalised"]
        numeric_drift.update(differences["numeric_type_drift_fields"])
        null_spelling.update(differences["null_type_spelling_fields"])
        reordered = reordered or differences["field_order_reordered"]
        before = view["before"]
        after = view["after"]
        raw_schemas[backend] = [before.schema, after.schema]
        for table in (before, after):
            for name in table.schema:
                if any(
                    row.get(name) is None or row.get(name) == ""
                    for row in table.rows
                ):
                    null_value_fields.add(name)
    for side_index in range(2):
        backends = list(raw_schemas)
        for index, left_backend in enumerate(backends):
            for right_backend in backends[index + 1:]:
                left_schema = raw_schemas[left_backend][side_index]
                right_schema = raw_schemas[right_backend][side_index]
                for name in sorted(set(left_schema) & set(right_schema)):
                    pair = {left_schema[name], right_schema[name]}
                    if pair <= {"int", "float"} and left_schema[name] != right_schema[name]:
                        numeric_drift.add(name)
                    if pair <= {"null", "str"} and left_schema[name] != right_schema[name]:
                        null_spelling.add(name)
    return {
        "rules": [
            "empty string and null cell values compare equal",
            "int and float values compare equal when numerically identical",
            "schema field order is ignored",
            "schema null and str type families are compatible for null-only columns",
        ],
        "numeric_type_drift_fields": sorted(numeric_drift),
        "null_type_spelling_fields": sorted(null_spelling),
        "field_order_reordered": reordered,
        "fields_with_null_or_empty_values": sorted(null_value_fields),
    }


def _view_public(view: dict, backend_name: str) -> dict:
    """Strip in-memory TableData objects before embedding a view in a report."""
    return {
        "backend": backend_name,
        "status": view["status"],
        "checks": view["checks"],
        "counts": view["counts"],
        "residual": view["residual"],
        "partition_checksum_mismatches": view["partition_checksum_mismatches"],
        "normalised_fingerprint": view["fingerprint"],
    }


def run_cross_backend_audit(
    config: Config,
    table: str,
    before_root: Path | str,
    after_root: Path | str,
    *,
    key: str | None = None,
    backend_names: tuple[str, ...] = CROSS_BACKEND_AUDIT_BACKENDS,
    clock: Callable[[], str] | None = None,
) -> dict:
    """Audit one before/after pair through every backend with shared semantics.

    Each backend reads both directories independently.  The per-backend checks
    run on normalised values, so backend spelling artefacts cannot change a
    verdict.  When the normalised verdicts still disagree, the report lands in
    the explicit ``cross_backend_conflict`` status and names the check, key or
    partition behind the disagreement.
    """
    if not NAME_RE.fullmatch(table):
        raise DataError(f"invalid table name: {table!r}")
    before_path = Path(before_root).resolve()
    after_path = Path(after_root).resolve()
    if not before_path.is_dir():
        raise NotFoundError(f"before table directory does not exist: {before_path}")
    if not after_path.is_dir():
        raise NotFoundError(f"after table directory does not exist: {after_path}")

    primary_key = key or config.table_key(table)
    views: dict[str, dict] = {}
    fingerprints: dict[str, str] = {}
    for backend_name in backend_names:
        backend = config.resolve_backend(backend_name)
        before = read_table_data(backend, before_path)
        after = read_table_data(backend, after_path)
        if primary_key not in before.schema or primary_key not in after.schema:
            raise ConfigError(
                f"primary key {primary_key!r} is not present in both table schemas",
                details={
                    "backend": backend_name,
                    "before_has_key": primary_key in before.schema,
                    "after_has_key": primary_key in after.schema,
                },
            )
        view = _normalised_audit_once(table, primary_key, before, after)
        views[backend.name] = view
        fingerprints[backend.name] = view["fingerprint"]

    conflicts = _check_conflicts(views, primary_key)
    statuses = {backend: view["status"] for backend, view in views.items()}
    unique_statuses = set(statuses.values())
    if conflicts or len(unique_statuses) > 1:
        status = CROSS_BACKEND_CONFLICT_STATUS
        summary = (
            "backends disagree after semantic normalisation; see "
            "cross_backend_conflicts for the check, key or partition involved"
        )
    else:
        status = next(iter(unique_statuses))
        summary = {
            "consistent": (
                "all backends agree after normalising nulls, numeric type drift "
                "and schema field order"
            ),
            "residual_diff": "all backends agree that row or checksum differences remain",
            "structural_mismatch": "all backends agree that schema or partition structure differs",
        }[status]

    # The fingerprint is identical whenever the normalised content is, so the
    # report id stays stable across backends and across repeated runs.
    unique_fingerprints = set(fingerprints.values())
    if len(unique_fingerprints) == 1:
        report_fingerprint = next(iter(unique_fingerprints))
    else:
        # Conflicting readings still get one deterministic id for this pair.
        report_fingerprint = sha256_text(
            canonical_json({"table": table, "key": primary_key,
                            "fingerprints": fingerprints})
        )[:16]

    reference = next(iter(views.values()))
    report = {
        "format": 1,
        "report_id": f"rpt-{report_fingerprint}",
        "table": table,
        "key": primary_key,
        "status": status,
        "generated_at": clock() if clock else config.now_iso(),
        "backend": "cross-backend",
        "before_root": str(before_path),
        "after_root": str(after_path),
        "checks": reference["checks"],
        "counts": reference["counts"],
        "residual": reference["residual"],
        "partition_checksum_mismatches": reference["partition_checksum_mismatches"],
        "summary": summary,
        "audit_mode": "cross_backend",
        "backends": sorted(views),
        "backend_views": {
            name: _view_public(view, name) for name, view in sorted(views.items())
        },
        "backend_statuses": statuses,
        "normalisation": _normalisation_notes(views),
        "normalised_fingerprints": fingerprints,
        "cross_backend_conflicts": conflicts,
    }
    json_path, markdown_path = write_audit_report(config.reports_dir, report)
    report["artifacts"] = {
        "json_report": str(json_path),
        "markdown_report": str(markdown_path),
    }
    return report
