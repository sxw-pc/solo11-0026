"""Immutable snapshot manifests and their deterministic content identifiers."""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any, Mapping

from lakeback._util import NAME_RE, canonical_json, normalise_iso, sha256_text
from lakeback.backends.base import Schema
from lakeback.errors import LedgerError

SCHEMA_TYPES = {"int", "float", "str", "bool", "null"}
SHA_RE = re.compile(r"^[a-f0-9]{64}$")
SNAPSHOT_RE = re.compile(r"^snap-[a-f0-9]{32}$")


def schema_as_list(schema: Schema) -> list[dict]:
    return [{"name": name, "type": kind} for name, kind in schema.items()]


def schema_from_manifest(items: list[dict]) -> Schema:
    return {item["name"]: item["type"] for item in items}


def file_entry(path: str, rows: int, size: int, sha256: str, partition: dict) -> dict:
    return {
        "path": path,
        "rows": rows,
        "size": size,
        "sha256": sha256,
        "partition": dict(partition),
    }


def _content_payload(
    table: str, backend: str, schema: Schema | list[dict], partitions: list[dict],
    files: list[dict],
) -> dict:
    schema_items = schema_as_list(schema) if isinstance(schema, dict) else schema
    normalised_files = [
        {
            "path": item["path"],
            "partition": dict(item["partition"]),
            "rows": int(item["rows"]),
            "sha256": item["sha256"],
            "size": int(item["size"]),
        }
        for item in files
    ]
    return {
        "table": table,
        "backend": backend,
        "schema": schema_items,
        "partitions": partitions,
        "files": normalised_files,
    }


def content_digest(
    table: str, backend: str, schema: Schema | list[dict], partitions: list[dict],
    files: list[dict],
) -> str:
    return sha256_text(canonical_json(_content_payload(table, backend, schema, partitions, files)))


def snapshot_id_for_content(
    table: str, backend: str, schema: Schema | list[dict], partitions: list[dict],
    files: list[dict],
) -> tuple[str, str]:
    digest = content_digest(table, backend, schema, partitions, files)
    return f"snap-{digest[:32]}", digest


def build_manifest(
    *,
    table: str,
    backend: str,
    schema: Schema,
    partitions: list[dict],
    files: list[dict],
    created_at: str,
) -> dict:
    snapshot_id, digest = snapshot_id_for_content(table, backend, schema, partitions, files)
    try:
        created = normalise_iso(created_at)
    except ValueError as exc:
        raise LedgerError(f"invalid snapshot created_at: {exc}") from exc
    return {
        "format": 1,
        "table": table,
        "snapshot_id": snapshot_id,
        "content_digest": digest,
        "parent_id": None,
        "version": None,
        "created_at": created,
        "backend": backend,
        "schema": schema_as_list(schema),
        "partitions": partitions,
        "files": files,
        "row_count": sum(int(item["rows"]) for item in files),
        "total_bytes": sum(int(item["size"]) for item in files),
        "file_count": len(files),
    }


def validate_manifest(manifest: Mapping[str, Any]) -> None:
    """Reject malformed or internally inconsistent snapshot metadata."""
    if not isinstance(manifest, Mapping):
        raise LedgerError("manifest must be a JSON object")
    required = {
        "table", "snapshot_id", "content_digest", "parent_id", "version", "created_at",
        "backend", "schema", "partitions", "files", "row_count", "total_bytes", "file_count",
    }
    missing = required - set(manifest)
    if missing:
        raise LedgerError(f"manifest missing fields: {', '.join(sorted(missing))}")
    if not isinstance(manifest["table"], str) or not NAME_RE.fullmatch(manifest["table"]):
        raise LedgerError("manifest contains an invalid table name")
    if not SNAPSHOT_RE.fullmatch(str(manifest["snapshot_id"])):
        raise LedgerError("manifest contains an invalid snapshot_id")
    if manifest["backend"] not in {"csv", "parquet"}:
        raise LedgerError("manifest contains an unsupported backend")
    if not isinstance(manifest["version"], int) or manifest["version"] < 1:
        raise LedgerError("manifest version must be a positive integer")
    parent = manifest["parent_id"]
    if parent is not None and not SNAPSHOT_RE.fullmatch(str(parent)):
        raise LedgerError("manifest parent_id is invalid")
    try:
        normalise_iso(manifest["created_at"])
    except ValueError as exc:
        raise LedgerError(f"manifest created_at is invalid: {exc}") from exc
    schema = manifest["schema"]
    if not isinstance(schema, list) or not schema:
        raise LedgerError("manifest schema must be a non-empty list")
    names: set[str] = set()
    for item in schema:
        if not isinstance(item, dict) or set(item) != {"name", "type"}:
            raise LedgerError("manifest schema entries must contain name and type")
        if not isinstance(item["name"], str) or item["name"] in names:
            raise LedgerError("manifest schema contains duplicate or invalid field names")
        if item["type"] not in SCHEMA_TYPES:
            raise LedgerError(f"manifest has unsupported field type {item['type']!r}")
        names.add(item["name"])
    files = manifest["files"]
    if not isinstance(files, list) or not files:
        raise LedgerError("manifest must list at least one data file")
    seen_paths: set[str] = set()
    for item in files:
        if not isinstance(item, dict):
            raise LedgerError("manifest files must be objects")
        path = item.get("path")
        if not isinstance(path, str) or not path:
            raise LedgerError("manifest file path must be a non-empty string")
        pure = PurePosixPath(path)
        if pure.is_absolute() or ".." in pure.parts or "\\" in path:
            raise LedgerError(f"manifest file path is unsafe: {path!r}")
        if path in seen_paths:
            raise LedgerError(f"manifest lists duplicate file path: {path}")
        seen_paths.add(path)
        if not SHA_RE.fullmatch(str(item.get("sha256"))):
            raise LedgerError(f"manifest file checksum is invalid: {path}")
        for numeric in ("rows", "size"):
            if not isinstance(item.get(numeric), int) or item[numeric] < 0:
                raise LedgerError(f"manifest file {numeric} is invalid: {path}")
        if not isinstance(item.get("partition"), dict):
            raise LedgerError(f"manifest file partition is invalid: {path}")
    if manifest["row_count"] != sum(item["rows"] for item in files):
        raise LedgerError("manifest row_count does not match file rows")
    if manifest["total_bytes"] != sum(item["size"] for item in files):
        raise LedgerError("manifest total_bytes does not match file sizes")
    if manifest["file_count"] != len(files):
        raise LedgerError("manifest file_count does not match files")
    expected_id, digest = snapshot_id_for_content(
        manifest["table"], manifest["backend"], schema, manifest["partitions"], files
    )
    if manifest["content_digest"] != digest or manifest["snapshot_id"] != expected_id:
        raise LedgerError(
            "manifest content checksum does not match its snapshot_id",
            details={"snapshot_id": manifest.get("snapshot_id")},
        )
