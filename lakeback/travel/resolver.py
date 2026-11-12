"""Resolve a version number or wall-clock instant to an immutable snapshot."""

from __future__ import annotations

import re
from typing import Any

from lakeback._util import NAME_RE, parse_timestamp, to_iso
from lakeback.errors import NotFoundError, RequestError
from lakeback.snapshot.ledger import Ledger

VERSION_RE = re.compile(r"^v?(\d+)$")


def parse_at(at: Any) -> tuple[str, int | str]:
    """Return ``('version', n)`` or ``('timestamp', iso)``."""
    if isinstance(at, int) and not isinstance(at, bool):
        if at < 1:
            raise RequestError("snapshot version must be positive")
        return "version", at
    text = str(at).strip()
    if not text:
        raise RequestError("snapshot selector 'at' must not be empty")
    if text.lower() == "latest":
        return "latest", "latest"
    match = VERSION_RE.fullmatch(text)
    if match:
        version = int(match.group(1))
        if version < 1:
            raise RequestError("snapshot version must be positive")
        return "version", version
    try:
        return "timestamp", to_iso(parse_timestamp(text))
    except ValueError as exc:
        raise RequestError(
            f"invalid snapshot selector {text!r}; expected a version number or ISO-8601 time"
        ) from exc


def resolve_snapshot(ledger: Ledger, table: str, at: Any) -> dict:
    """Resolve exact versions and the latest snapshot at or before a timestamp."""
    if not NAME_RE.fullmatch(table):
        raise RequestError(f"invalid table name: {table!r}")
    history = ledger.snapshots(table)
    if not history:
        raise NotFoundError(f"table {table!r} has no snapshots")
    mode, value = parse_at(at)
    if mode == "version":
        for manifest in history:
            if manifest["version"] == value:
                return manifest
        versions = ", ".join(str(item["version"]) for item in history)
        raise NotFoundError(
            f"table {table!r} has no snapshot version {value}",
            details={"available_versions": versions},
        )
    if mode == "latest":
        return history[-1]
    boundary = parse_timestamp(value)
    eligible = [
        item for item in history
        if parse_timestamp(item["created_at"]) <= boundary
    ]
    if not eligible:
        first = history[0]["created_at"]
        raise NotFoundError(
            f"no snapshot for table {table!r} exists at or before {value}",
            details={"earliest_snapshot": first},
        )
    eligible.sort(key=lambda item: (parse_timestamp(item["created_at"]), item["version"]))
    return eligible[-1]
