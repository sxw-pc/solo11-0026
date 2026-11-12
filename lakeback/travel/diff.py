"""Deterministic, primary-key-aligned diff between two snapshots."""

from __future__ import annotations

from typing import Any

from lakeback._util import ensure_unique, primary_key_sort
from lakeback.config import Config
from lakeback.errors import DataError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.manifest import schema_from_manifest
from lakeback.travel.reader import read_snapshot_rows
from lakeback.travel.resolver import resolve_snapshot


def _keyed(rows: list[dict], key: str) -> dict[Any, dict]:
    try:
        ensure_unique(rows, key)
    except ValueError as exc:
        raise DataError(str(exc)) from exc
    return {row[key]: row for row in rows}


def _summary(manifest: dict) -> dict:
    return {
        "version": manifest["version"],
        "snapshot_id": manifest["snapshot_id"],
        "created_at": manifest["created_at"],
        "row_count": manifest["row_count"],
    }


def compute_diff(
    table: str,
    from_manifest: dict,
    to_manifest: dict,
    from_rows: list[dict],
    to_rows: list[dict],
    key: str,
    from_schema: dict,
    to_schema: dict,
) -> dict:
    """Compare rows by key; every output list is sorted by primary key."""
    if key not in from_schema or key not in to_schema:
        raise DataError(f"primary key {key!r} is missing from one snapshot schema")
    before = _keyed(from_rows, key)
    after = _keyed(to_rows, key)
    added_keys = sorted(after.keys() - before.keys(), key=primary_key_sort)
    removed_keys = sorted(before.keys() - after.keys(), key=primary_key_sort)
    shared_keys = sorted(before.keys() & after.keys(), key=primary_key_sort)
    field_order = list(from_schema)
    field_order.extend(name for name in to_schema if name not in from_schema)
    modified: list[dict] = []
    unchanged = 0
    for row_key in shared_keys:
        left = before[row_key]
        right = after[row_key]
        changes = [
            {"name": name, "before": left.get(name), "after": right.get(name)}
            for name in field_order
            if left.get(name) != right.get(name)
        ]
        if changes:
            modified.append({"key": row_key, "fields": changes})
        else:
            unchanged += 1
    return {
        "table": table,
        "key": key,
        "from": _summary(from_manifest),
        "to": _summary(to_manifest),
        "schema_changed": from_schema != to_schema,
        "counts": {
            "from_rows": len(from_rows),
            "to_rows": len(to_rows),
            "added": len(added_keys),
            "removed": len(removed_keys),
            "modified": len(modified),
            "unchanged": unchanged,
        },
        "added": [after[row_key] for row_key in added_keys],
        "removed": [before[row_key] for row_key in removed_keys],
        "modified": modified,
    }


def diff_versions(
    config: Config, ledger: Ledger, table: str, from_at: Any, to_at: Any
) -> dict:
    from_manifest = resolve_snapshot(ledger, table, from_at)
    to_manifest = resolve_snapshot(ledger, table, to_at)
    from_rows, from_schema = read_snapshot_rows(config, from_manifest)
    to_rows, to_schema = read_snapshot_rows(config, to_manifest)
    return compute_diff(
        table,
        from_manifest,
        to_manifest,
        from_rows,
        to_rows,
        config.table_key(table),
        from_schema,
        to_schema,
    )
