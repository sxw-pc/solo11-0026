"""State normalisation and half-open interval compression for entity history."""

from __future__ import annotations

from typing import Any

from lakeback._util import canonical_json
from lakeback.backends.base import coerce_value
from lakeback.errors import DataError

PRESENT = "present"
ABSENT = "absent"
UNVERIFIABLE = "unverifiable"


def key_candidates(raw: Any, kinds: list[str]) -> list[Any]:
    """Typed spellings for a CLI/API key value, without assuming one schema type."""
    values: list[Any] = []
    if not isinstance(raw, str):
        values.append(raw)
    for kind in kinds:
        try:
            values.append(coerce_value(raw, kind))
        except DataError:
            continue
    if isinstance(raw, str):
        values.append(raw)
    if not values:  # pragma: no cover - defensive
        values.append(raw)
    unique: dict[str, Any] = {}
    for value in values:
        unique.setdefault(canonical_json(value), value)
    return list(unique.values())


def state_for(info: dict, row: dict | None) -> dict:
    """Describe one entity at one version; unknown is distinct from absent."""
    if not info["readable"]:
        # A snapshot that cannot be verified must never be reported as a
        # deleted row.  Each unverifiable version keeps its own identity so two
        # different failures are never compressed into one interval.
        return {
            "version": info["version"],
            "status": UNVERIFIABLE,
            "identity": (UNVERIFIABLE, info["version"]),
            "fields": None,
            "error": dict(info["error"]),
        }
    if row is None:
        return {
            "version": info["version"],
            "status": ABSENT,
            "identity": (ABSENT,),
            "fields": None,
            "error": None,
        }
    fields = dict(row)
    return {
        "version": info["version"],
        "status": PRESENT,
        "identity": (PRESENT, canonical_json(fields)),
        "fields": fields,
        "error": None,
    }


def compress_intervals(states: list[dict], infos: list[dict], stop: int) -> list[dict]:
    """Compress equal adjacent states; ``stop`` is exclusive (upper + 1)."""
    if not states:
        return []
    manifests = {info["version"]: info["manifest"] for info in infos}
    intervals: list[dict] = []

    def close(state: dict, start: int, end: int) -> None:
        manifest = manifests[start]
        interval = {
            "status": state["status"],
            "present": (
                True if state["status"] == PRESENT
                else False if state["status"] == ABSENT
                else None
            ),
            "valid_from": start,
            "valid_to": end,
            "source_version": start,
            "source_snapshot": manifest["snapshot_id"],
            "fields": dict(state["fields"]) if state["fields"] is not None else None,
        }
        if state["error"] is not None:
            interval["error"] = dict(state["error"])
        intervals.append(interval)

    start = states[0]["version"]
    identity = states[0]["identity"]
    current = states[0]
    for state in states[1:]:
        if state["identity"] == identity:
            # Adjacent equal states merge, but never across a deletion or an
            # unverifiable version: those identities differ by construction.
            continue
        close(current, start, state["version"])
        start = state["version"]
        identity = state["identity"]
        current = state
    close(current, start, stop)
    return intervals


def key_for_marker(infos: list[dict], key_field: str, marker: str) -> Any:
    for info in infos:
        if info["readable"]:
            row = info["rows"].get(marker)
            if row is not None:
                return row[key_field]
    return None  # pragma: no cover - markers come from readable versions
