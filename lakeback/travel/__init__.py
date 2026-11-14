"""Time-travel resolution, reading, version diffing and referential audits."""

from lakeback.travel.diff import compute_diff, diff_versions
from lakeback.travel.reader import (
    pin_snapshot,
    read_pinned_snapshot,
    read_snapshot_rows,
    read_view,
)
from lakeback.travel.referential import audit_referential_integrity
from lakeback.travel.resolver import resolve_snapshot

__all__ = [
    "audit_referential_integrity",
    "compute_diff",
    "diff_versions",
    "pin_snapshot",
    "read_pinned_snapshot",
    "read_snapshot_rows",
    "read_view",
    "resolve_snapshot",
]
