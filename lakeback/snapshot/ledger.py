"""Append-only JSONL snapshot ledger with a verifiable derived index."""

from __future__ import annotations

import copy
import json
import os
import threading
from pathlib import Path

from lakeback._util import canonical_json, sha256_text
from lakeback.errors import LedgerError, NotFoundError
from lakeback.snapshot.manifest import validate_manifest

LEDGER_FILE = "snapshots.jsonl"
INDEX_FILE = "index.json"


class Ledger:
    """A small, local, tamper-evident history of immutable snapshots."""

    def __init__(self, ledger_dir: Path, *, read_only: bool = False) -> None:
        self.directory = Path(ledger_dir).resolve()
        self.ledger_path = self.directory / LEDGER_FILE
        self.index_path = self.directory / INDEX_FILE
        self.read_only = read_only
        self._lock = threading.Lock()
        self._records: list[dict] = []
        self._by_table: dict[str, list[dict]] = {}
        self._by_id: dict[str, dict] = {}
        if not read_only:
            try:
                self.directory.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise LedgerError(
                    f"cannot create ledger directory {self.directory}: {exc}"
                ) from exc
        self._load()
        expected = self._build_index()
        if self.index_path.exists():
            actual = self._read_index()
            if actual != expected:
                raise LedgerError(
                    "snapshot index is inconsistent with the append-only ledger",
                    details={"path": str(self.index_path)},
                )
        elif not read_only:
            self._write_index(expected)

    def _load(self) -> None:
        if not self.ledger_path.exists():
            return
        try:
            text = self.ledger_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise LedgerError(f"cannot read ledger {self.ledger_path}: {exc}") from exc
        previous_hash = ""
        version_by_table: dict[str, int] = {}
        last_id_by_table: dict[str, str] = {}
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                raise LedgerError(f"blank line in ledger at line {line_number}")
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise LedgerError(f"invalid JSON in ledger line {line_number}") from exc
            if not isinstance(record, dict) or set(record) != {
                "manifest", "previous_hash", "record_hash"
            }:
                raise LedgerError(f"malformed ledger record at line {line_number}")
            if record["previous_hash"] != previous_hash:
                raise LedgerError(f"snapshot hash chain is broken at line {line_number}")
            expected_hash = sha256_text(previous_hash + canonical_json(record["manifest"]))
            if record["record_hash"] != expected_hash:
                raise LedgerError(f"snapshot record was modified at line {line_number}")
            manifest = record["manifest"]
            try:
                validate_manifest(manifest)
            except LedgerError as exc:
                raise LedgerError(f"invalid snapshot manifest at line {line_number}: {exc}") from exc
            table = manifest["table"]
            if version_by_table.get(table, 0) + 1 != manifest["version"]:
                raise LedgerError(f"non-sequential version for table {table!r}")
            expected_parent = last_id_by_table.get(table)
            if manifest["parent_id"] != expected_parent:
                raise LedgerError(f"broken parent chain for table {table!r}")
            if manifest["snapshot_id"] in self._by_id:
                raise LedgerError(f"duplicate snapshot_id in ledger: {manifest['snapshot_id']}")
            self._records.append(record)
            self._by_id[manifest["snapshot_id"]] = manifest
            self._by_table.setdefault(table, []).append(manifest)
            version_by_table[table] = manifest["version"]
            last_id_by_table[table] = manifest["snapshot_id"]
            previous_hash = record["record_hash"]

    def _read_index(self) -> dict:
        try:
            return json.loads(self.index_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LedgerError(f"cannot read snapshot index {self.index_path}: {exc}") from exc

    def _build_index(self) -> dict:
        tables: dict[str, dict] = {}
        snapshots: dict[str, dict] = {}
        for record in self._records:
            manifest = record["manifest"]
            table = manifest["table"]
            entry = {
                "version": manifest["version"],
                "snapshot_id": manifest["snapshot_id"],
                "parent_id": manifest["parent_id"],
                "created_at": manifest["created_at"],
                "row_count": manifest["row_count"],
            }
            bucket = tables.setdefault(table, {"latest_version": 0, "snapshots": []})
            bucket["snapshots"].append(entry)
            bucket["latest_version"] = manifest["version"]
            snapshots[manifest["snapshot_id"]] = {
                "table": table,
                "version": manifest["version"],
                "created_at": manifest["created_at"],
            }
        return {
            "format": 1,
            "record_count": len(self._records),
            "last_hash": self._records[-1]["record_hash"] if self._records else "",
            "tables": tables,
            "snapshots": snapshots,
        }

    def _write_index(self, index: dict) -> None:
        temporary = self.index_path.with_suffix(".json.tmp")
        try:
            temporary.write_text(
                json.dumps(index, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary, self.index_path)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise LedgerError(f"cannot update snapshot index {self.index_path}: {exc}") from exc

    def append(self, manifest: dict) -> dict:
        """Append a new content manifest, or return the existing idempotent record."""
        if self.read_only:
            raise LedgerError("ledger was opened read-only; cannot append snapshots")
        if not isinstance(manifest, dict):
            raise LedgerError("snapshot manifest must be an object")
        snapshot_id = manifest.get("snapshot_id")
        if snapshot_id in self._by_id:
            return copy.deepcopy(self._by_id[snapshot_id])
        candidate = copy.deepcopy(manifest)
        table = candidate.get("table")
        if not isinstance(table, str):
            raise LedgerError("snapshot manifest is missing table")
        history = self._by_table.get(table, [])
        candidate["format"] = 1
        candidate["version"] = history[-1]["version"] + 1 if history else 1
        candidate["parent_id"] = history[-1]["snapshot_id"] if history else None
        validate_manifest(candidate)
        with self._lock:
            if candidate["snapshot_id"] in self._by_id:
                return copy.deepcopy(self._by_id[candidate["snapshot_id"]])
            previous_hash = (
                self._records[-1]["record_hash"] if self._records else ""
            )
            record_hash = sha256_text(previous_hash + canonical_json(candidate))
            record = {
                "manifest": candidate,
                "previous_hash": previous_hash,
                "record_hash": record_hash,
            }
            try:
                with self.ledger_path.open("a", encoding="utf-8", newline="\n") as handle:
                    handle.write(canonical_json(record) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as exc:
                raise LedgerError(f"cannot append snapshot ledger: {exc}") from exc
            self._records.append(record)
            self._by_id[candidate["snapshot_id"]] = candidate
            self._by_table.setdefault(table, []).append(candidate)
            self._write_index(self._build_index())
        return copy.deepcopy(candidate)

    def snapshots(self, table: str | None = None) -> list[dict]:
        if table is None:
            return [copy.deepcopy(record["manifest"]) for record in self._records]
        return copy.deepcopy(self._by_table.get(table, []))

    def get_snapshot(self, snapshot_id: str) -> dict:
        try:
            return copy.deepcopy(self._by_id[snapshot_id])
        except KeyError as exc:
            raise NotFoundError(f"snapshot not found: {snapshot_id}") from exc

    def get_version(self, table: str, version: int) -> dict:
        for manifest in self._by_table.get(table, []):
            if manifest["version"] == version:
                return copy.deepcopy(manifest)
        raise NotFoundError(f"table {table!r} has no snapshot version {version}")

    def tables(self) -> dict[str, dict]:
        return {table: copy.deepcopy(self._build_index()["tables"][table])
                for table in sorted(self._by_table)}

    def index_view(self) -> dict:
        return copy.deepcopy(self._build_index())

    def verify(self) -> None:
        """Re-read from disk and compare; useful for diagnostics."""
        fresh = type(self)(self.directory, read_only=self.read_only)
        if fresh.index_view() != self.index_view():  # pragma: no cover - defensive
            raise LedgerError("ledger changed while verifying")
