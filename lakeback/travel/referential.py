"""Cross-table referential-integrity audit between two pinned snapshots.

Unlike :func:`lakeback.travel.diff.compute_diff`, this compares rows of two
*different* tables: every fact row's foreign key must resolve to a parent row in
an explicitly pinned dimension snapshot.  Keys are typed tuples normalised with
each side's own manifest schema; they are never concatenated into strings.
"""

from __future__ import annotations

from typing import Any, Mapping

from lakeback._util import primary_key_sort
from lakeback.backends.base import coerce_value
from lakeback.config import Config
from lakeback.errors import DataError, RequestError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.manifest import schema_from_manifest
from lakeback.travel.reader import (
    history_digest,
    pin_snapshot,
    read_pinned_snapshot,
    verify_pinned_history,
    verify_snapshot_files,
)

NULL_POLICIES = ("skip", "error")
_NUMERIC_TYPES = {"int", "float"}


def _comparison_type(
    fact_column: str, fact_type: str, dimension_column: str, dimension_type: str
) -> str:
    """Resolve two schema types to the domain used for tuple equality.

    ``int``/``float`` share the exact numeric domain: each side is normalised
    with its own declared type and Python's exact cross-type ``==``/``hash``
    (``10 == 10.0`` but ``2**53 + 1 != float(2**53)``) is used for lookup.  A
    common ``float`` cast is deliberately avoided because it would round large
    integer keys and create false matches.
    """
    if fact_type == dimension_type:
        return fact_type
    pair = {fact_type, dimension_type}
    if "null" in pair:
        return (pair - {"null"}).pop()
    if pair <= _NUMERIC_TYPES:
        return "numeric"
    raise DataError(
        "incompatible foreign-key types between snapshots",
        details={
            "fact_column": fact_column,
            "fact_type": fact_type,
            "dimension_column": dimension_column,
            "dimension_type": dimension_type,
        },
    )


def _validate_key(
    foreign_key: Mapping[str, str], fact_schema: dict, dimension_schema: dict
) -> list[dict]:
    """Validate column presence and type compatibility; preserve pair order."""
    if not isinstance(foreign_key, Mapping) or not foreign_key:
        raise RequestError(
            "foreign key must map at least one fact column to a dimension column"
        )
    pairs: list[dict] = []
    seen_fact: set[str] = set()
    seen_dimension: set[str] = set()
    for fact_column, dimension_column in foreign_key.items():
        if not isinstance(fact_column, str) or not isinstance(dimension_column, str):
            raise RequestError("foreign key column names must be strings")
        fact_column, dimension_column = fact_column.strip(), dimension_column.strip()
        if not fact_column or not dimension_column:
            raise RequestError("foreign key column names must be non-empty")
        if fact_column in seen_fact:
            raise RequestError(f"duplicate foreign key column: {fact_column!r}")
        if dimension_column in seen_dimension:
            raise RequestError(f"duplicate parent key column: {dimension_column!r}")
        seen_fact.add(fact_column)
        seen_dimension.add(dimension_column)
        fact_type = fact_schema.get(fact_column)
        dimension_type = dimension_schema.get(dimension_column)
        if fact_type is None or dimension_type is None:
            side = "fact" if fact_type is None else "dimension"
            raise DataError(
                f"foreign key column {fact_column!r} -> {dimension_column!r} "
                f"is missing from the {side} snapshot schema",
                details={
                    "fact_column": fact_column,
                    "dimension_column": dimension_column,
                    "missing_from": side,
                    "fact_schema": sorted(fact_schema),
                    "dimension_schema": sorted(dimension_schema),
                },
            )
        pairs.append({
            "fact_column": fact_column,
            "fact_type": fact_type,
            "dimension_column": dimension_column,
            "dimension_type": dimension_type,
            "compare_type": _comparison_type(
                fact_column, fact_type, dimension_column, dimension_type
            ),
        })
    return pairs


def _key_type(pair: dict, side: str) -> str:
    """Per-side normalisation type; numeric pairs keep their declared type."""
    if pair["compare_type"] == "numeric":
        return pair[f"{side}_type"]
    return pair["compare_type"]


def _normalise(value: Any, kind: str, column: str) -> Any:
    try:
        return coerce_value(value, kind)
    except DataError as exc:
        raise DataError(f"cannot normalise column {column!r} as {kind}: {exc}") from exc


def _key_for(row: Mapping[str, Any], pairs: list[dict], side: str) -> tuple:
    """Build one typed tuple; components stay separate, never concatenated.

    Each component is normalised with *its own side's* declared schema type.
    For an int/float pair this keeps integer keys exact, while Python's exact
    cross-type equality still matches ``10`` with ``10.0``.
    """
    return tuple(
        _normalise(row.get(pair[f"{side}_column"]), _key_type(pair, side),
                   pair[f"{side}_column"])
        for pair in pairs
    )


def _key_sort(key: tuple) -> tuple:
    return tuple(primary_key_sort(component) for component in key)


def _parent_index(
    rows: list[dict], pairs: list[dict]
) -> tuple[dict[tuple, list[int]], int]:
    """Index parent rows by typed tuple; NULL components are not candidates."""
    index: dict[tuple, list[int]] = {}
    null_rows = 0
    for position, row in enumerate(rows):
        key = _key_for(row, pairs, "dimension")
        if any(component is None for component in key):
            null_rows += 1
            continue
        index.setdefault(key, []).append(position)
    return index, null_rows


def _duplicate_keys(index: dict[tuple, list[int]]) -> list[tuple]:
    return sorted(
        (key for key, positions in index.items() if len(positions) > 1),
        key=_key_sort,
    )


def _classify_fact_rows(
    fact_rows: list[dict],
    pairs: list[dict],
    index: dict[tuple, list[int]],
    duplicates: set[tuple],
    null_policy: str,
) -> dict:
    orphans: list[dict] = []
    ambiguous_matches: list[dict] = []
    null_rows: list[int] = []
    matched = 0
    fact_columns = [pair["fact_column"] for pair in pairs]
    for position, row in enumerate(fact_rows):
        key = _key_for(row, pairs, "fact")
        if any(component is None for component in key):
            null_rows.append(position)
            continue
        parents = index.get(key)
        if not parents:
            orphans.append({
                "row_index": position,
                "foreign_key": dict(zip(fact_columns, key)),
                "row": row,
            })
        elif key in duplicates:
            # Never silently pick one parent or degrade to many-to-one.
            ambiguous_matches.append({
                "row_index": position,
                "foreign_key": dict(zip(fact_columns, key)),
                "row": row,
                "parent_row_indexes": list(parents),
            })
        else:
            matched += 1
    if null_policy == "error" and null_rows:
        raise DataError(
            "foreign key contains NULL components but null_policy is 'error'",
            details={
                "null_rows": len(null_rows),
                "sample_row_indexes": null_rows[:10],
            },
        )
    orphans.sort(
        key=lambda item: (_key_sort(tuple(item["foreign_key"].values())),
                          item["row_index"])
    )
    ambiguous_matches.sort(
        key=lambda item: (_key_sort(tuple(item["foreign_key"].values())),
                          item["row_index"])
    )
    return {
        "orphans": orphans,
        "ambiguous_matches": ambiguous_matches,
        "null_skipped": len(null_rows),
        "matched": matched,
        "ambiguous_parent_matches": len(ambiguous_matches),
    }


def _check(name: str, passed: bool, evidence: dict) -> dict:
    return {"name": name, "status": "pass" if passed else "fail", "evidence": evidence}


def _status(orphan_count: int, duplicate_count: int) -> str:
    if orphan_count and duplicate_count:
        return "orphan_references_and_duplicate_parent_keys"
    if orphan_count:
        return "orphan_references"
    if duplicate_count:
        return "duplicate_parent_keys"
    return "consistent"


def _snapshot_summary(manifest: dict, schema: dict) -> dict:
    return {
        "table": manifest["table"],
        "version": manifest["version"],
        "snapshot_id": manifest["snapshot_id"],
        "created_at": manifest["created_at"],
        "row_count": manifest["row_count"],
        "schema": schema,
    }


def audit_referential_integrity(
    config: Config,
    ledger: Ledger,
    fact_table: str,
    fact_version: Any,
    dimension_table: str,
    dimension_version: Any,
    foreign_key: Mapping[str, str],
    *,
    null_policy: str = "skip",
) -> dict:
    """Audit one pinned fact snapshot against one pinned dimension snapshot.

    ``foreign_key`` maps fact columns to dimension columns in composite-key
    order.  Both versions must be explicit (integers or ``vN``); ``latest`` and
    timestamps are rejected and the current lake table is never read.  Both
    manifests are pinned before either snapshot is read; afterwards every
    selected file's sha256 is recomputed and the ledger history up to each
    pinned version is proven unchanged.
    """
    if null_policy not in NULL_POLICIES:
        raise RequestError(
            f"null_policy must be one of {', '.join(NULL_POLICIES)}, "
            f"got {null_policy!r}"
        )
    fact_manifest = pin_snapshot(ledger, fact_table, fact_version)
    dimension_manifest = pin_snapshot(ledger, dimension_table, dimension_version)
    fact_schema = schema_from_manifest(fact_manifest["schema"])
    dimension_schema = schema_from_manifest(dimension_manifest["schema"])
    # Reject missing columns / incompatible types before materialising any rows.
    pairs = _validate_key(foreign_key, fact_schema, dimension_schema)

    expected_history = {
        (fact_table, fact_manifest["version"]):
            history_digest(ledger, fact_table, fact_manifest["version"]),
        (dimension_table, dimension_manifest["version"]):
            history_digest(ledger, dimension_table, dimension_manifest["version"]),
    }
    _, fact_rows, read_fact_schema = read_pinned_snapshot(
        config, ledger, fact_table, fact_manifest["version"], manifest=fact_manifest
    )
    _, dimension_rows, read_dimension_schema = read_pinned_snapshot(
        config, ledger, dimension_table, dimension_manifest["version"],
        manifest=dimension_manifest,
    )
    if read_fact_schema != fact_schema or read_dimension_schema != dimension_schema:
        raise DataError("snapshot rows were read with a different schema")
    files_verified = {
        "fact": verify_snapshot_files(config, fact_manifest),
        "dimension": verify_snapshot_files(config, dimension_manifest),
    }
    integrity = verify_pinned_history(
        ledger, [fact_manifest, dimension_manifest], expected_history
    )

    index, dimension_null_rows = _parent_index(dimension_rows, pairs)
    duplicate_keys = _duplicate_keys(index)
    duplicate_set = set(duplicate_keys)
    outcome = _classify_fact_rows(
        fact_rows, pairs, index, duplicate_set, null_policy
    )
    orphans = outcome["orphans"]
    matched = outcome["matched"]
    ambiguous = outcome["ambiguous_parent_matches"]
    null_skipped = outcome["null_skipped"]
    checked = matched + ambiguous + len(orphans)
    dimension_columns = [pair["dimension_column"] for pair in pairs]
    parent_key_duplicates = [
        {
            "key": dict(zip(dimension_columns, key)),
            "values": list(key),
            "row_count": len(index[key]),
            "row_indexes": list(index[key]),
            "rows": [dimension_rows[position] for position in index[key]],
        }
        for key in duplicate_keys
    ]

    checks = [
        _check(
            "row_accounting",
            matched + ambiguous + len(orphans) + null_skipped == len(fact_rows),
            {
                "fact_rows": len(fact_rows),
                "matched": matched,
                "ambiguous_parent_matches": ambiguous,
                "orphans": len(orphans),
                "null_skipped": null_skipped,
            },
        ),
        _check(
            "referential_integrity",
            not orphans,
            {"orphan_references": len(orphans)},
        ),
        _check(
            "parent_key_uniqueness",
            not parent_key_duplicates,
            {
                "duplicate_parent_keys": len(parent_key_duplicates),
                "duplicate_parent_rows": sum(
                    item["row_count"] for item in parent_key_duplicates
                ),
            },
        ),
    ]

    counts = {
        "fact_rows": len(fact_rows),
        "dimension_rows": len(dimension_rows),
        "checked": checked,
        "matched": matched,
        "ambiguous_parent_matches": ambiguous,
        "orphans": len(orphans),
        "null_skipped": null_skipped,
        "dimension_null_key_rows": dimension_null_rows,
        "parent_key_duplicates": len(parent_key_duplicates),
        "parent_key_duplicate_rows": sum(
            item["row_count"] for item in parent_key_duplicates
        ),
    }
    return {
        "format": 1,
        "audit": "referential_integrity",
        "status": _status(len(orphans), len(parent_key_duplicates)),
        "fact": _snapshot_summary(fact_manifest, fact_schema),
        "dimension": _snapshot_summary(dimension_manifest, dimension_schema),
        "foreign_key": pairs,
        "null_policy": null_policy,
        "counts": counts,
        "checks": checks,
        "orphans": orphans,
        "ambiguous_matches": outcome["ambiguous_matches"],
        "parent_key_duplicates": parent_key_duplicates,
        "verified": {
            "snapshot_files_sha256": files_verified,
            "history_unchanged": integrity["history_unchanged"],
            "ledger_record_count": integrity["record_count"],
            "history_digests": integrity["history_digests"],
        },
    }
