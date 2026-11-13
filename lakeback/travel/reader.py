"""Read-only time-travel views with partition pruning and integrity checks."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from lakeback._util import (
    canonical_json,
    partition_tuple,
    primary_key_sort,
    safe_child,
    sha256_file,
    stable_rows,
)
from lakeback.backends import get_backend
from lakeback.backends.base import matches_partition
from lakeback.config import Config
from lakeback.errors import BackendError, DataError, NotFoundError, RequestError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.manifest import schema_from_manifest
from lakeback.travel.resolver import resolve_snapshot


def _snapshot_summary(manifest: dict) -> dict:
    return {
        "table": manifest["table"],
        "version": manifest["version"],
        "snapshot_id": manifest["snapshot_id"],
        "parent_id": manifest["parent_id"],
        "created_at": manifest["created_at"],
        "row_count": manifest["row_count"],
        "file_count": manifest["file_count"],
        "partitions": manifest["partitions"],
    }


def read_snapshot_rows(
    config: Config,
    manifest: dict,
    *,
    partitions: Mapping[str, str] | None = None,
    verify: bool = True,
) -> tuple[list[dict], dict]:
    """Read only the manifest files matching the requested partition values."""
    try:
        backend = get_backend(manifest["backend"])
    except ValueError as exc:  # pragma: no cover - manifests are validated
        raise BackendError(str(exc)) from exc
    if not backend.available():
        raise BackendError(
            f"snapshot {manifest['snapshot_id']} needs the {backend.name} backend: "
            f"{backend.availability_hint}"
        )
    schema = schema_from_manifest(manifest["schema"])
    manifest_partitions = {partition_tuple(item) for item in manifest["partitions"]}
    available_columns = {name for partition in manifest["partitions"] for name in partition}
    for name in partitions or {}:
        if name not in available_columns:
            raise RequestError(f"unknown partition column: {name!r}")
    snapshot_root = safe_child(config.snapshot_store, manifest["snapshot_id"])
    rows: list[dict] = []
    for item in sorted(manifest["files"], key=lambda entry: entry["path"]):
        if not matches_partition(item["partition"], partitions):
            continue
        if partition_tuple(item["partition"]) not in manifest_partitions:
            raise DataError(
                f"snapshot file partition is not listed in the manifest: {item['path']}",
                details={"snapshot_id": manifest["snapshot_id"]},
            )
        path = safe_child(snapshot_root, item["path"])
        if not path.is_file():
            raise NotFoundError(
                f"snapshot data file is missing: {item['path']}",
                details={"snapshot_id": manifest["snapshot_id"]},
            )
        if verify and sha256_file(path) != item["sha256"]:
            raise BackendError(f"snapshot data file checksum mismatch: {item['path']}")
        file_rows = backend.read_rows(path, schema)
        if len(file_rows) != item["rows"]:
            raise DataError(
                f"snapshot data file row count does not match manifest: {item['path']}",
                details={
                    "snapshot_id": manifest["snapshot_id"],
                    "expected": item["rows"],
                    "actual": len(file_rows),
                },
            )
        rows.extend(file_rows)
    return rows, schema


def _sort_rows(rows: list[dict], schema: dict, key: str | None) -> list[dict]:
    if key and key in schema:
        return stable_rows(rows, key)
    ordered = sorted(rows, key=canonical_json)
    return ordered


_TYPE_CHECKS = {
    "int": lambda value: isinstance(value, int) and not isinstance(value, bool),
    "float": lambda value: isinstance(value, float) and not isinstance(value, bool),
    "bool": lambda value: isinstance(value, bool),
    "str": lambda value: isinstance(value, str),
}


def _conform_rows(
    rows: list[dict], schema: dict, manifest: dict, *, expected_total: int | None
) -> list[dict]:
    """Ensure the read view matches the manifest schema and (optionally) row count.

    The manifest is the ledger's record of truth.  A full read must return
    exactly its schema and row_count; a partition-filtered read must still
    expose the same fields with the same types even when it returns fewer rows.
    """
    expected_fields = list(schema)
    for row in rows:
        if list(row) != expected_fields:
            raise DataError(
                "snapshot rows do not match the manifest schema",
                details={
                    "snapshot_id": manifest["snapshot_id"],
                    "expected_fields": expected_fields,
                    "actual_fields": list(row),
                },
            )
        for name, kind in schema.items():
            value = row[name]
            check = _TYPE_CHECKS.get(kind)
            if value is not None and check is not None and not check(value):
                raise DataError(
                    f"snapshot value for {name!r} does not match manifest type {kind}",
                    details={"snapshot_id": manifest["snapshot_id"], "value": value},
                )
    if expected_total is not None and len(rows) != expected_total:
        raise DataError(
            "snapshot row count does not match the manifest",
            details={
                "snapshot_id": manifest["snapshot_id"],
                "expected": expected_total,
                "actual": len(rows),
            },
        )
    return rows


def read_view(
    config: Config,
    ledger: Ledger,
    table: str,
    at,
    *,
    partitions: Mapping[str, str] | None = None,
    limit: int | None = None,
    offset: int = 0,
    verify: bool = True,
) -> dict:
    manifest = resolve_snapshot(ledger, table, at)
    rows, schema = read_snapshot_rows(config, manifest, partitions=partitions, verify=verify)
    expected_total = None
    if not partitions:
        expected_total = manifest["row_count"]
    rows = _conform_rows(rows, schema, manifest, expected_total=expected_total)
    key = config.table_keys.get(table)
    ordered = _sort_rows(rows, schema, key)
    total = len(ordered)
    if offset < 0:
        raise RequestError("offset must be non-negative")
    if limit is not None and limit < 0:
        raise RequestError("limit must be non-negative")
    page = ordered[offset:] if limit is None else ordered[offset: offset + limit]
    return {
        "table": table,
        "at": str(at),
        "snapshot": _snapshot_summary(manifest),
        "partition_filters": dict(partitions or {}),
        "schema": schema,
        "total_rows": total,
        "offset": offset,
        "limit": limit,
        "returned": len(page),
        "rows": page,
    }
