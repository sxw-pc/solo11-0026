"""Read-only time-travel views with partition pruning and integrity checks."""

from __future__ import annotations

from typing import Mapping

from lakeback._util import (
    canonical_json,
    safe_child,
    sha256_file,
    sha256_text,
    stable_rows,
)
from lakeback.backends import get_backend
from lakeback.backends.base import matches_partition
from lakeback.config import Config
from lakeback.errors import BackendError, LedgerError, NotFoundError, RequestError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.manifest import schema_from_manifest
from lakeback.travel.resolver import VERSION_RE, resolve_snapshot


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
    available_columns = {name for partition in manifest["partitions"] for name in partition}
    for name in partitions or {}:
        if name not in available_columns:
            raise RequestError(f"unknown partition column: {name!r}")
    snapshot_root = safe_child(config.snapshot_store, manifest["snapshot_id"])
    rows: list[dict] = []
    for item in sorted(manifest["files"], key=lambda entry: entry["path"]):
        if not matches_partition(item["partition"], partitions):
            continue
        path = safe_child(snapshot_root, item["path"])
        if not path.is_file():
            raise NotFoundError(
                f"snapshot data file is missing: {item['path']}",
                details={"snapshot_id": manifest["snapshot_id"]},
            )
        if verify and sha256_file(path) != item["sha256"]:
            raise BackendError(f"snapshot data file checksum mismatch: {item['path']}")
        rows.extend(backend.read_rows(path, schema))
    return rows, schema


def _pinned_version(at) -> int:
    """Require an explicit integer snapshot version; never ``latest``."""
    if isinstance(at, bool):
        raise RequestError("snapshot version must be an integer, not a boolean")
    if isinstance(at, int):
        if at < 1:
            raise RequestError("snapshot version must be positive")
        return at
    text = "" if at is None else str(at).strip()
    if text.lower() == "latest":
        raise RequestError(
            "an explicit snapshot version is required; 'latest' is not allowed"
        )
    match = VERSION_RE.fullmatch(text)
    if not match:
        raise RequestError(
            f"an explicit snapshot version (integer or vN) is required, got {at!r}"
        )
    version = int(match.group(1))
    if version < 1:
        raise RequestError("snapshot version must be positive")
    return version


def read_pinned_snapshot(
    config: Config,
    ledger: Ledger,
    table: str,
    version,
    *,
    manifest: dict | None = None,
    partitions: Mapping[str, str] | None = None,
) -> tuple[dict, list[dict], dict]:
    """Read one exact, already-fixed version and re-verify it end to end.

    The manifest is resolved once from the given version (never from ``latest``
    or the current lake table); a caller that pinned the manifest earlier may
    pass it in so both sides of a two-table read share one resolved snapshot.
    After the rows are materialised the ledger is re-checked without mutating
    it: the pinned snapshot must still resolve to the same manifest and the
    recorded history digest must be unchanged.
    """
    pinned = _pinned_version(version)
    if manifest is None:
        manifest = resolve_snapshot(ledger, table, pinned)
    elif manifest.get("table") != table or manifest.get("version") != pinned:
        raise LedgerError(
            f"pinned manifest {manifest.get('snapshot_id')} is not {table!r} v{pinned}",
            details={"table": table, "version": pinned},
        )
    history_before = _history_digest(ledger, table, pinned)
    rows, schema = read_snapshot_rows(config, manifest, partitions=partitions, verify=True)
    _verify_unchanged(ledger, table, manifest, history_before)
    return manifest, rows, schema


def pin_snapshot(ledger: Ledger, table: str, version) -> dict:
    """Resolve exactly one explicit version; reject ``latest`` and timestamps."""
    pinned = _pinned_version(version)
    manifest = resolve_snapshot(ledger, table, pinned)
    if manifest["version"] != pinned:  # pragma: no cover - resolver guarantees this
        raise LedgerError(
            f"resolved version {manifest['version']} does not match pinned {pinned}",
            details={"table": table},
        )
    return manifest


def _history_digest(ledger: Ledger, table: str, upto_version: int | None = None) -> str:
    """Fingerprint history up to and including ``upto_version``.

    Appending newer versions must not invalidate a pinned read, so only the
    prefix the caller pinned is hashed; rewriting any of those entries changes
    the digest.
    """
    history = [
        (item["version"], item["snapshot_id"], item["created_at"], item["parent_id"])
        for item in ledger.snapshots(table)
        if upto_version is None or item["version"] <= upto_version
    ]
    return sha256_text(canonical_json(history))


def history_digest(ledger: Ledger, table: str, upto_version: int | None = None) -> str:
    """Fingerprint the immutable version history of one table."""
    return _history_digest(ledger, table, upto_version)


def verify_snapshot_files(
    config: Config,
    manifest: dict,
    *,
    partitions: Mapping[str, str] | None = None,
) -> int:
    """Recompute sha256 for every selected snapshot file; return the count."""
    snapshot_root = safe_child(config.snapshot_store, manifest["snapshot_id"])
    verified = 0
    for item in sorted(manifest["files"], key=lambda entry: entry["path"]):
        if not matches_partition(item["partition"], partitions):
            continue
        path = safe_child(snapshot_root, item["path"])
        if not path.is_file():
            raise NotFoundError(
                f"snapshot data file is missing: {item['path']}",
                details={"snapshot_id": manifest["snapshot_id"]},
            )
        if sha256_file(path) != item["sha256"]:
            raise BackendError(f"snapshot data file checksum mismatch: {item['path']}")
        verified += 1
    return verified


def _verify_unchanged(
    ledger: Ledger, table: str, manifest: dict, history_before: str
) -> None:
    """Fail closed if the pinned snapshot or its history moved during the read."""
    fresh = Ledger(ledger.directory)
    if _history_digest(fresh, table, manifest["version"]) != history_before:
        raise LedgerError(
            f"snapshot history for table {table!r} changed during the read",
            details={"snapshot_id": manifest["snapshot_id"]},
        )
    current = resolve_snapshot(fresh, table, manifest["version"])
    if current["snapshot_id"] != manifest["snapshot_id"]:
        raise LedgerError(
            f"pinned snapshot for table {table!r} was rewritten during the read",
            details={"version": manifest["version"]},
        )


def verify_pinned_history(
    ledger: Ledger, pinned: list[dict], expected: Mapping[tuple[str, int], str]
) -> dict:
    """Re-read the ledger from disk and prove no pinned history was rewritten."""
    fresh = Ledger(ledger.directory)
    digests: dict[str, str] = {}
    for manifest in pinned:
        table = manifest["table"]
        version = manifest["version"]
        digest = _history_digest(fresh, table, version)
        if digest != expected[(table, version)]:
            raise LedgerError(
                f"snapshot history for table {table!r} changed during the read",
                details={"snapshot_id": manifest["snapshot_id"], "version": version},
            )
        current = resolve_snapshot(fresh, table, version)
        if current["snapshot_id"] != manifest["snapshot_id"]:
            raise LedgerError(
                f"pinned snapshot for table {table!r} was rewritten during the read",
                details={"version": version},
            )
        digests[f"{table}@{version}"] = digest
    return {
        "record_count": len(fresh.snapshots()),
        "history_unchanged": True,
        "history_digests": digests,
        "tables": sorted({manifest["table"] for manifest in pinned}),
    }


def _sort_rows(rows: list[dict], schema: dict, key: str | None) -> list[dict]:
    if key and key in schema:
        return stable_rows(rows, key)
    ordered = sorted(rows, key=canonical_json)
    return ordered


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
