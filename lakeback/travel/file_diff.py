"""File-level diff between two snapshots of the same table.

Unlike :mod:`lakeback.travel.diff`, which aligns rows by primary key, this
module attributes changes purely from manifest file entries: relative path,
partition annotation, byte size and sha256.

Classification rules (path + hash together, so an in-place rewrite is never
reported as a delete plus an add):

* relative path and sha256 both unchanged      -> ``unchanged``
* path only in the old snapshot                -> ``deleted``
* path only in the new snapshot                -> ``added``
* same path, different sha256                  -> ``rewritten``

For every rewritten file the old content sha256 is looked up among the new
snapshot's files to spot a reuse opportunity (identical bytes materialised at
another relative path).  Reuse is only credited inside the same partition
annotation; content that moved to another partition or disappeared entirely is
reported with the reason it cannot be reused.
"""

from __future__ import annotations

from typing import Any

from lakeback._util import partition_tuple
from lakeback.snapshot.ledger import Ledger
from lakeback.travel.resolver import resolve_snapshot

CATEGORIES = ("added", "deleted", "rewritten", "unchanged")
BYTE_BUCKETS = ("added", "deleted", "rewritten_old", "rewritten_new", "unchanged")
REUSE_REASONS = ("partition_mismatch", "content_absent")


def _summary(manifest: dict) -> dict:
    return {
        "version": manifest["version"],
        "snapshot_id": manifest["snapshot_id"],
        "created_at": manifest["created_at"],
        "row_count": manifest["row_count"],
        "file_count": manifest["file_count"],
        "total_bytes": manifest["total_bytes"],
    }


def _descriptor(item: dict) -> dict:
    return {
        "path": item["path"],
        "rows": int(item["rows"]),
        "size": int(item["size"]),
        "sha256": item["sha256"],
        "partition": dict(item["partition"]),
    }


def _partition_columns(manifest: dict) -> set[str]:
    columns: set[str] = set()
    for item in manifest["files"]:
        columns.update(item["partition"])
    return columns


def _reuse_for_rewrite(old_item: dict, to_by_sha: dict[str, list[dict]]) -> dict:
    """Decide whether the old bytes of a rewritten file are reusable."""
    old_sha = old_item["sha256"]
    old_partition = partition_tuple(old_item["partition"])
    locations = sorted(to_by_sha.get(old_sha, []), key=lambda item: item["path"])
    content_locations = [
        {"path": item["path"], "partition": dict(item["partition"])} for item in locations
    ]
    if not locations:
        return {
            "status": "content_absent",
            "reusable_bytes": 0,
            "matched_path": None,
            "matched_partition": None,
            "content_locations": [],
            "reason": "old content sha256 does not appear in any file of the new snapshot",
        }
    same_partition = [
        item for item in locations
        if partition_tuple(item["partition"]) == old_partition
    ]
    if same_partition:
        match = same_partition[0]
        return {
            "status": "reusable",
            "reusable_bytes": int(old_item["size"]),
            "matched_path": match["path"],
            "matched_partition": dict(match["partition"]),
            "content_locations": content_locations,
            "reason": "old content is materialised at another relative path in the same partition",
        }
    match = locations[0]
    return {
        "status": "partition_mismatch",
        "reusable_bytes": 0,
        "matched_path": match["path"],
        "matched_partition": dict(match["partition"]),
        "content_locations": content_locations,
        "reason": "old content exists in the new snapshot only under different partition annotations",
    }


def _empty_partition_bucket(partition: dict) -> dict:
    return {
        "partition": dict(partition),
        "counts": {name: 0 for name in CATEGORIES},
        "bytes": {name: 0 for name in BYTE_BUCKETS},
    }


def compute_file_diff(table: str, from_manifest: dict, to_manifest: dict) -> dict:
    """Attribute file-level changes between two manifests of one table."""
    old_files = sorted(from_manifest["files"], key=lambda item: item["path"])
    new_files = sorted(to_manifest["files"], key=lambda item: item["path"])
    old_by_path = {item["path"]: item for item in old_files}
    new_by_path = {item["path"]: item for item in new_files}
    to_by_sha: dict[str, list[dict]] = {}
    for item in new_files:
        to_by_sha.setdefault(item["sha256"], []).append(item)

    added: list[dict] = []
    deleted: list[dict] = []
    unchanged: list[dict] = []
    rewritten: list[dict] = []
    annotation_drifts: list[dict] = []

    for old_item in old_files:
        new_item = new_by_path.get(old_item["path"])
        if new_item is None:
            deleted.append(_descriptor(old_item))
            continue
        if new_item["sha256"] == old_item["sha256"]:
            # Path and hash unchanged: unchanged regardless of annotation drift.
            if partition_tuple(new_item["partition"]) != partition_tuple(old_item["partition"]):
                annotation_drifts.append({
                    "type": "partition_annotation_drift",
                    "path": old_item["path"],
                    "from_partition": dict(old_item["partition"]),
                    "to_partition": dict(new_item["partition"]),
                })
            unchanged.append(_descriptor(old_item))
            continue
        # Same relative path, different sha256: one in-place rewrite.
        if partition_tuple(new_item["partition"]) != partition_tuple(old_item["partition"]):
            annotation_drifts.append({
                "type": "partition_annotation_drift",
                "path": old_item["path"],
                "from_partition": dict(old_item["partition"]),
                "to_partition": dict(new_item["partition"]),
            })
        rewritten.append({
            "path": old_item["path"],
            "partition": dict(old_item["partition"]),
            "from": _descriptor(old_item),
            "to": _descriptor(new_item),
            "reuse": _reuse_for_rewrite(old_item, to_by_sha),
        })
    for new_item in new_files:
        if new_item["path"] not in old_by_path:
            added.append(_descriptor(new_item))

    from_columns = _partition_columns(from_manifest)
    to_columns = _partition_columns(to_manifest)
    consistent = from_columns == to_columns
    unattributable: list[dict] = []
    if not consistent:
        unattributable.append({
            "type": "partition_column_drift",
            "from_partition_columns": sorted(from_columns),
            "to_partition_columns": sorted(to_columns),
            "only_in_from": sorted(from_columns - to_columns),
            "only_in_to": sorted(to_columns - from_columns),
            "message": (
                "snapshots use different partition columns; per-partition rollup "
                "cannot be aligned, file-level attribution is still provided"
            ),
        })
        # Per-file annotation drift is implied by the column-level drift and
        # would only add noise; the descriptors retain every annotation.
        annotation_drifts = []

    buckets: dict[tuple, dict] = {}
    if consistent:
        def bucket_for(partition: dict) -> dict:
            key = partition_tuple(partition)
            return buckets.setdefault(key, _empty_partition_bucket(partition))

        for item in added:
            bucket = bucket_for(item["partition"])
            bucket["counts"]["added"] += 1
            bucket["bytes"]["added"] += item["size"]
        for item in deleted:
            bucket = bucket_for(item["partition"])
            bucket["counts"]["deleted"] += 1
            bucket["bytes"]["deleted"] += item["size"]
        for item in unchanged:
            bucket = bucket_for(item["partition"])
            bucket["counts"]["unchanged"] += 1
            bucket["bytes"]["unchanged"] += item["size"]
        for item in rewritten:
            # Stable path identity: old bytes are attributed to the old partition.
            bucket = bucket_for(item["from"]["partition"])
            bucket["counts"]["rewritten"] += 1
            bucket["bytes"]["rewritten_old"] += item["from"]["size"]
            bucket["bytes"]["rewritten_new"] += item["to"]["size"]
    partitions = [buckets[key] for key in sorted(buckets)]

    reason_tally = {
        reason: {"files": 0, "bytes": 0} for reason in REUSE_REASONS
    }
    reusable_bytes = 0
    for item in rewritten:
        reuse = item["reuse"]
        if reuse["status"] == "reusable":
            reusable_bytes += reuse["reusable_bytes"]
        else:
            entry = reason_tally[reuse["status"]]
            entry["files"] += 1
            entry["bytes"] += item["from"]["size"]
    non_reusable_bytes = sum(entry["bytes"] for entry in reason_tally.values())

    return {
        "table": table,
        "from": _summary(from_manifest),
        "to": _summary(to_manifest),
        "partition_attribution": {
            "status": "ok" if consistent else "inconsistent",
            "from_partition_columns": sorted(from_columns),
            "to_partition_columns": sorted(to_columns),
            "annotation_drifts": annotation_drifts,
            "unattributable": unattributable,
        },
        "counts": {
            "added": len(added),
            "deleted": len(deleted),
            "rewritten": len(rewritten),
            "unchanged": len(unchanged),
        },
        "bytes": {
            "added": sum(item["size"] for item in added),
            "deleted": sum(item["size"] for item in deleted),
            "rewritten_old": sum(item["from"]["size"] for item in rewritten),
            "rewritten_new": sum(item["to"]["size"] for item in rewritten),
            "unchanged": sum(item["size"] for item in unchanged),
        },
        "partitions": partitions,
        "files": {
            "added": added,
            "deleted": deleted,
            "rewritten": rewritten,
            "unchanged": unchanged,
        },
        "reuse": {
            "reusable_files": sum(
                1 for item in rewritten if item["reuse"]["status"] == "reusable"
            ),
            "reusable_bytes": reusable_bytes,
            "non_reusable_files": sum(
                entry["files"] for entry in reason_tally.values()
            ),
            "non_reusable_bytes": non_reusable_bytes,
            "reasons": reason_tally,
        },
    }


def file_diff_versions(
    ledger: Ledger, table: str, from_at: Any, to_at: Any
) -> dict:
    """Resolve two snapshots and diff their manifest file entries."""
    from_manifest = resolve_snapshot(ledger, table, from_at)
    to_manifest = resolve_snapshot(ledger, table, to_at)
    return compute_file_diff(table, from_manifest, to_manifest)
