"""Per-entity state history across a contiguous range of snapshot versions."""

from __future__ import annotations

from typing import Any

from lakeback._util import NAME_RE, canonical_json, ensure_unique, primary_key_sort
from lakeback.config import Config
from lakeback.errors import (
    DataError,
    LakeBackError,
    LedgerError,
    NotFoundError,
    RequestError,
)
from lakeback.snapshot.ledger import Ledger
from lakeback.travel.reader import read_snapshot_rows
from lakeback.travel.resolver import parse_at
from lakeback.travel.states import (
    PRESENT,
    UNVERIFIABLE,
    compress_intervals,
    key_candidates,
    key_for_marker,
    state_for,
)

COMPLETE = "complete"
INCOMPLETE = "incomplete"


def _resolve_version(table: str, selector: Any, latest: int, label: str) -> int:
    """Resolve an inclusive range boundary; only versions (or latest) allowed."""
    mode, value = parse_at(selector)
    if mode == "latest":
        return latest
    if mode != "version":
        raise RequestError(
            f"{label} must be a snapshot version number or 'latest'; "
            "timestamps are not valid range boundaries"
        )
    if value > latest:
        raise NotFoundError(
            f"table {table!r} has no snapshot version {value}",
            details={"latest_version": latest},
        )
    return value


def _error_payload(exc: LakeBackError) -> dict:
    return {"code": exc.code, "message": exc.message, "details": dict(exc.details)}


def _read_version(config: Config, manifest: dict, key_field: str) -> dict:
    """Read and validate one version without aborting the whole history read."""
    info = {
        "manifest": manifest,
        "version": manifest["version"],
        "schema": None,
        "rows": {},
        "readable": False,
        "error": None,
    }
    try:
        # Reuse the fixed-snapshot reader so file presence and sha256 are checked.
        rows, schema = read_snapshot_rows(config, manifest, verify=True)
        details = {"version": manifest["version"], "snapshot_id": manifest["snapshot_id"]}
        if key_field not in schema:
            raise DataError(
                f"primary key {key_field!r} is missing from snapshot schema",
                details=details,
            )
        try:
            ensure_unique(rows, key_field)
        except ValueError as exc:
            raise DataError(str(exc), details=details) from exc
        info["schema"] = schema
        info["rows"] = {canonical_json(row[key_field]): row for row in rows}
        info["readable"] = True
    except LakeBackError as exc:
        payload = _error_payload(exc)
        payload["details"].setdefault("version", manifest["version"])
        payload["details"].setdefault("snapshot_id", manifest["snapshot_id"])
        info["error"] = payload
    except OSError as exc:
        info["error"] = {
            "code": "backend_error",
            "message": f"cannot read snapshot data file: {exc}",
            "details": {
                "version": manifest["version"],
                "snapshot_id": manifest["snapshot_id"],
            },
        }
    return info


def _version_entry(info: dict) -> dict:
    entry = {
        "version": info["version"],
        "snapshot_id": info["manifest"]["snapshot_id"],
        "created_at": info["manifest"]["created_at"],
        "readable": info["readable"],
    }
    if info["error"] is not None:
        entry["error"] = dict(info["error"])
    return entry


def _unverifiable_entry(info: dict) -> dict:
    return {
        "version": info["version"],
        "snapshot_id": info["manifest"]["snapshot_id"],
        "created_at": info["manifest"]["created_at"],
        "status": UNVERIFIABLE,
        "error": dict(info["error"]),
    }


def _selected_manifests(history: list[dict], first: int, last: int) -> list[dict]:
    manifests = [
        manifest for manifest in history if first <= manifest["version"] <= last
    ]
    versions = [manifest["version"] for manifest in manifests]
    if versions != list(range(first, last + 1)):
        raise LedgerError(
            "selected ledger versions are not contiguous",
            details={"expected": [first, last], "actual": versions},
        )
    return manifests


def _expanded_entities(infos: list[dict], key_field: str, last: int) -> list[dict]:
    """One entity per key seen in any readable version of the range."""
    markers = sorted(
        {
            marker
            for info in infos if info["readable"]
            for marker in info["rows"]
        },
        key=lambda marker: (
            primary_key_sort(key_for_marker(infos, key_field, marker)), marker
        ),
    )
    entities: list[dict] = []
    for marker in markers:
        states = [
            state_for(info, info["rows"].get(marker) if info["readable"] else None)
            for info in infos
        ]
        entities.append({
            "key": key_for_marker(infos, key_field, marker),
            "intervals": compress_intervals(states, infos, last + 1),
        })
    return entities


def _single_entity(
    infos: list[dict], key_field: str, key_value: Any, kinds: list[str], last: int
) -> list[dict]:
    candidates = [canonical_json(value) for value in key_candidates(key_value, kinds)]

    def lookup(info: dict) -> dict | None:
        if not info["readable"]:
            return None
        for marker in candidates:
            row = info["rows"].get(marker)
            if row is not None:
                return row
        return None

    states = [state_for(info, lookup(info)) for info in infos]
    present = [state for state in states if state["status"] == PRESENT]
    if present:
        key = present[0]["fields"][key_field]
    elif any(state["status"] == UNVERIFIABLE for state in states):
        # Not found in any readable version, but some version could not be
        # checked: report unknown rather than an empty "never existed" history.
        key = key_candidates(key_value, kinds)[0]
    else:
        return []
    return [{"key": key, "intervals": compress_intervals(states, infos, last + 1)}]


def entity_history(
    config: Config,
    ledger: Ledger,
    table: str,
    from_version: Any,
    to_version: Any,
    *,
    key_value: Any = None,
    key: str | None = None,
) -> dict:
    """Return compressed per-entity state intervals for one table.

    ``from_version`` and ``to_version`` are inclusive snapshot versions.  Every
    interval is half-open over version numbers: ``valid_from`` is included and
    ``valid_to`` is excluded.  The final interval ends at ``to_version + 1``.
    When ``key_value`` is omitted, every entity present in at least one readable
    version of the range is returned, ordered by primary key.
    """
    if not NAME_RE.fullmatch(table):
        raise RequestError(f"invalid table name: {table!r}")
    history = ledger.snapshots(table)
    if not history:
        raise NotFoundError(f"table {table!r} has no snapshots")
    latest = history[-1]["version"]
    first = _resolve_version(table, from_version, latest, "from")
    last = _resolve_version(table, to_version, latest, "to")
    if first > last:
        raise RequestError("from version must not be greater than to version")
    key_field = config.table_key(table, key)

    manifests = _selected_manifests(history, first, last)
    infos = [_read_version(config, manifest, key_field) for manifest in manifests]
    unverifiable = [_unverifiable_entry(info) for info in infos if not info["readable"]]

    key_kinds: list[str] = []
    for info in infos:
        if not info["readable"]:
            continue
        kind = info["schema"].get(key_field)
        if kind is not None and kind not in key_kinds:
            key_kinds.append(kind)

    if key_value is None:
        entities = _expanded_entities(infos, key_field, last)
        reported_key = None
    else:
        entities = _single_entity(infos, key_field, key_value, key_kinds, last)
        reported_key = entities[0]["key"] if entities else key_value
    entities.sort(
        key=lambda item: (primary_key_sort(item["key"]), canonical_json(item["key"]))
    )
    return {
        "table": table,
        "key": key_field,
        "key_value": reported_key,
        "from_version": first,
        "to_version": last,
        "status": COMPLETE if not unverifiable else INCOMPLETE,
        "complete": not unverifiable,
        "versions": [_version_entry(info) for info in infos],
        "unverifiable_versions": unverifiable,
        "entity_count": len(entities),
        "entities": entities,
    }
