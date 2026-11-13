"""Shared snapshot identification for the lineage CLI and HTTP routes."""

from __future__ import annotations

from lakeback.errors import RequestError
from lakeback.snapshot.manifest import SNAPSHOT_RE
from lakeback.travel.resolver import resolve_snapshot


def resolve_target(ledger, snapshot: str | None = None, table: str | None = None, at=None) -> dict:
    """Resolve an explicit snapshot id or a table/version selector to a manifest."""
    if snapshot and table:
        raise RequestError("identify a snapshot with either snapshot or table/at, not both")
    if snapshot:
        if not SNAPSHOT_RE.fullmatch(snapshot):
            raise RequestError(f"invalid snapshot id: {snapshot!r}")
        return ledger.get_snapshot(snapshot)
    if not table:
        raise RequestError("identify a snapshot with snapshot=<id> or table=<name> and at=<version>")
    if at is None:
        raise RequestError("a snapshot selector (at) is required together with table")
    return resolve_snapshot(ledger, table, at)
