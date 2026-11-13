"""Append-only, hash-chained lineage records stored next to the snapshot ledger."""

from __future__ import annotations

import copy
import json
import os
import threading
from pathlib import Path
from typing import Mapping

from lakeback._util import NAME_RE, canonical_json, parse_timestamp, sha256_text
from lakeback.errors import LedgerError, NotFoundError, RequestError
from lakeback.snapshot.manifest import SNAPSHOT_RE

LINEAGE_FILE = "lineage.jsonl"

_ENTRY_KEYS = {"snapshot_id", "table", "version", "sources", "created_at"}
_RECORD_KEYS = {"entry", "previous_hash", "record_hash"}
_SOURCE_KEYS = {"table", "version"}


def parse_source_spec(raw: str) -> dict:
    """Parse one ``table=version`` declaration from the CLI or HTTP layer."""
    text = str(raw).strip()
    name, separator, version_text = text.partition("=")
    if not separator or not name.strip() or not version_text.strip():
        raise RequestError(f"invalid source spec: {raw!r} (expected table=version)")
    table = name.strip()
    if not NAME_RE.fullmatch(table):
        raise RequestError(f"invalid source table name: {table!r}")
    if not version_text.strip().isdigit() or int(version_text) < 1:
        raise RequestError(f"invalid source version in {raw!r}; expected a positive integer")
    return {"table": table, "version": int(version_text)}


def _positive_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def canonical_sources(sources) -> list[dict]:
    """Validate, de-duplicate and sort source declarations into canonical form."""
    if not isinstance(sources, list):
        raise RequestError("lineage sources must be a list")
    unique: dict[tuple, dict] = {}
    for item in sources:
        if not isinstance(item, dict) or set(item) != _SOURCE_KEYS:
            raise RequestError("lineage sources must be objects with table and version")
        table = item["table"]
        if not isinstance(table, str) or not NAME_RE.fullmatch(table):
            raise RequestError(f"invalid source table name: {table!r}")
        if not _positive_int(item["version"]):
            raise RequestError(f"invalid source version for table {table!r}")
        unique[(table, item["version"])] = {"table": table, "version": item["version"]}
    return [unique[key] for key in sorted(unique)]


def _validate_entry(entry, line_number: int) -> None:
    if not isinstance(entry, dict) or set(entry) != _ENTRY_KEYS:
        raise LedgerError(f"malformed lineage entry at line {line_number}")
    if not SNAPSHOT_RE.fullmatch(str(entry["snapshot_id"])):
        raise LedgerError(f"lineage entry has an invalid snapshot_id at line {line_number}")
    if not isinstance(entry["table"], str) or not NAME_RE.fullmatch(entry["table"]):
        raise LedgerError(f"lineage entry has an invalid table name at line {line_number}")
    if not _positive_int(entry["version"]):
        raise LedgerError(f"lineage entry has an invalid version at line {line_number}")
    try:
        canonical = canonical_sources(entry["sources"])
    except RequestError as exc:
        raise LedgerError(f"invalid lineage sources at line {line_number}: {exc.message}") from exc
    if canonical != entry["sources"]:
        raise LedgerError(f"lineage sources are not canonical at line {line_number}")
    try:
        parse_timestamp(entry["created_at"])
    except ValueError as exc:
        raise LedgerError(f"lineage entry created_at is invalid at line {line_number}") from exc


def verify_lineage_file(ledger_dir, snapshots_by_id: Mapping) -> tuple[list[dict], str]:
    """Read and fully verify ``lineage.jsonl``; raise LedgerError on any problem.

    Returns the verified entries and the hash of the last record. Every record
    is checked at load time: JSON shape, hash chain, canonical entry form and a
    cross-reference against the snapshot ledger. Sources are *not* required to
    resolve; dangling references are legitimate and reported by queries.
    """
    path = Path(ledger_dir) / LINEAGE_FILE
    entries: list[dict] = []
    if not path.exists():
        return entries, ""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise LedgerError(f"cannot read lineage ledger {path}: {exc}") from exc
    previous_hash = ""
    seen: set[str] = set()
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            raise LedgerError(f"blank line in lineage ledger at line {line_number}")
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise LedgerError(f"invalid JSON in lineage ledger line {line_number}") from exc
        if not isinstance(record, dict) or set(record) != _RECORD_KEYS:
            raise LedgerError(f"malformed lineage record at line {line_number}")
        if record["previous_hash"] != previous_hash:
            raise LedgerError(f"lineage hash chain is broken at line {line_number}")
        expected_hash = sha256_text(previous_hash + canonical_json(record["entry"]))
        if record["record_hash"] != expected_hash:
            raise LedgerError(f"lineage record was modified at line {line_number}")
        entry = record["entry"]
        _validate_entry(entry, line_number)
        snapshot_id = entry["snapshot_id"]
        if snapshot_id in seen:
            raise LedgerError(f"duplicate lineage record for snapshot {snapshot_id}")
        seen.add(snapshot_id)
        manifest = snapshots_by_id.get(snapshot_id)
        if manifest is None:
            raise LedgerError(f"lineage record references unknown snapshot {snapshot_id}")
        if manifest["table"] != entry["table"] or manifest["version"] != entry["version"]:
            raise LedgerError(f"lineage record does not match snapshot {snapshot_id}")
        entries.append(copy.deepcopy(entry))
        previous_hash = record["record_hash"]
    return entries, previous_hash


class LineageStore:
    """Registration and read access for snapshot lineage records."""

    def __init__(self, ledger_dir, ledger) -> None:
        self.directory = Path(ledger_dir).resolve()
        self.path = self.directory / LINEAGE_FILE
        self._ledger = ledger
        self._lock = threading.Lock()
        snapshots = {m["snapshot_id"]: m for m in ledger.snapshots()}
        self._entries, self._last_hash = verify_lineage_file(self.directory, snapshots)
        self._by_snapshot = {entry["snapshot_id"]: entry for entry in self._entries}

    def records(self) -> list[dict]:
        return copy.deepcopy(self._entries)

    def record_for(self, snapshot_id: str) -> dict | None:
        entry = self._by_snapshot.get(snapshot_id)
        return copy.deepcopy(entry) if entry is not None else None

    def register(self, snapshot_id: str, sources, created_at: str) -> dict:
        """Append a lineage record, or return the existing identical one."""
        manifest = self._ledger.get_snapshot(snapshot_id)
        canonical = canonical_sources(list(sources))
        try:
            parse_timestamp(created_at)
        except ValueError as exc:
            raise RequestError(f"invalid lineage timestamp: {created_at!r}") from exc
        with self._lock:
            existing = self._by_snapshot.get(snapshot_id)
            if existing is not None:
                if existing["sources"] != canonical:
                    raise RequestError(
                        f"lineage already registered for {snapshot_id} with different sources",
                        details={"snapshot_id": snapshot_id},
                    )
                return self._result(existing, created=False)
            entry = {
                "snapshot_id": snapshot_id,
                "table": manifest["table"],
                "version": manifest["version"],
                "sources": canonical,
                "created_at": created_at,
            }
            record = {
                "entry": entry,
                "previous_hash": self._last_hash,
                "record_hash": sha256_text(self._last_hash + canonical_json(entry)),
            }
            try:
                with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                    handle.write(canonical_json(record) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as exc:
                raise LedgerError(f"cannot append lineage ledger: {exc}") from exc
            self._entries.append(entry)
            self._by_snapshot[snapshot_id] = entry
            self._last_hash = record["record_hash"]
            return self._result(entry, created=True)

    def _result(self, entry: dict, created: bool) -> dict:
        return {
            "snapshot_id": entry["snapshot_id"],
            "table": entry["table"],
            "version": entry["version"],
            "sources": copy.deepcopy(entry["sources"]),
            "created": created,
            "unresolved": [s for s in entry["sources"] if not self._version_exists(s)],
            "recorded_at": entry["created_at"],
        }

    def _version_exists(self, source: dict) -> bool:
        try:
            self._ledger.get_version(source["table"], source["version"])
            return True
        except NotFoundError:
            return False
