"""Create immutable snapshots from a local lake table directory."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Callable

from lakeback._util import NAME_RE, partition_tuple, safe_child, sha256_file
from lakeback.config import Config
from lakeback.errors import DataError, LedgerError, NotFoundError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.manifest import build_manifest, file_entry
from lakeback.tables import discover_schema


def _ordered_partitions(entries) -> list[dict]:
    layouts = {tuple(sorted(entry.partition)) for entry in entries}
    if len(layouts) != 1:
        raise DataError(
            "table files use inconsistent partition directory layouts",
            details={"layouts": [list(layout) for layout in sorted(layouts)]},
        )
    unique = {partition_tuple(entry.partition): dict(entry.partition) for entry in entries}
    return [unique[key] for key in sorted(unique)]


def _copy_into_store(source: Path, target: Path, expected_sha: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and sha256_file(target) == expected_sha:
        return
    temporary = target.with_name(target.name + ".tmp")
    try:
        shutil.copyfile(source, temporary)
        if sha256_file(temporary) != expected_sha:
            raise LedgerError(f"checksum changed while copying {source}")
        os.replace(temporary, target)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise LedgerError(f"cannot materialise snapshot file {target}: {exc}") from exc


def create_snapshot(
    config: Config,
    table: str,
    *,
    table_root: Path | str | None = None,
    backend_name: str | None = None,
    clock: Callable[[], str] | None = None,
    ledger: Ledger | None = None,
) -> dict:
    """Snapshot all data files of one table and append the manifest to the ledger."""
    if not NAME_RE.fullmatch(table):
        raise DataError(f"invalid table name: {table!r}")
    backend = config.resolve_backend(backend_name)
    root = Path(table_root).resolve() if table_root else (config.lake_root / table)
    if not root.exists():
        raise NotFoundError(f"table directory does not exist: {root}")
    if not root.is_dir():
        raise DataError(f"table path is not a directory: {root}")

    schema, entries = discover_schema(backend, root)
    files: list[dict] = []
    for entry in entries:
        rows = backend.read_rows(entry.path, schema)
        checksum = backend.checksum(entry.path)
        files.append(
            file_entry(
                path=entry.relative,
                rows=len(rows),
                size=entry.path.stat().st_size,
                sha256=checksum,
                partition=entry.partition,
            )
        )
    partitions = _ordered_partitions(entries)
    manifest = build_manifest(
        table=table,
        backend=backend.name,
        schema=schema,
        partitions=partitions,
        files=files,
        created_at=(clock() if clock else config.now_iso()),
    )

    ledger = ledger or Ledger(config.ledger_dir)
    # Materialise the immutable copy before consulting the ledger.  Identical
    # content therefore always targets the same snapshot_id directory; an
    # interrupted or damaged earlier copy is repaired instead of silently
    # reused, and the ledger append below performs the idempotent decision.
    snapshot_root = safe_child(config.snapshot_store, manifest["snapshot_id"])
    by_path = {entry.relative: entry for entry in entries}
    for item in files:  # ``files`` and ``entries`` share the same relative paths
        entry = by_path[item["path"]]
        target = safe_child(snapshot_root, item["path"])
        _copy_into_store(entry.path, target, item["sha256"])
    return ledger.append(manifest)
