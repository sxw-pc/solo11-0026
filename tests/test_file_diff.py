from __future__ import annotations

import hashlib
import json
import shutil

import pytest

from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.manifest import build_manifest
from lakeback.snapshot.snapshotter import create_snapshot
from lakeback.travel.file_diff import compute_file_diff, file_diff_versions


def _sha(tag: str) -> str:
    return hashlib.sha256(tag.encode("utf-8")).hexdigest()


def _entry(path: str, size: int, sha: str, partition: dict) -> dict:
    return {
        "path": path,
        "rows": size,
        "size": size,
        "sha256": sha,
        "partition": dict(partition),
    }


def _manifest(version_files: list[dict], *, partition_columns=("dt",)) -> dict:
    partitions: list[dict] = []
    seen: set[tuple] = set()
    for item in version_files:
        key = tuple(sorted(item["partition"].items()))
        if key not in seen:
            seen.add(key)
            partitions.append(dict(item["partition"]))
    partitions.sort(key=lambda item: tuple(item[name] for name in partition_columns))
    schema = [{"name": "order_id", "type": "int"}]
    for name in partition_columns:
        schema.append({"name": name, "type": "str"})
    manifest = build_manifest(
        table="orders",
        backend="csv",
        schema={item["name"]: item["type"] for item in schema},
        partitions=partitions,
        files=version_files,
        created_at="2025-01-01T00:00:00Z",
    )
    return manifest


def _rollup(result, partition: dict) -> dict:
    for bucket in result["partitions"]:
        if bucket["partition"] == partition:
            return bucket
    raise AssertionError(f"no rollup for partition {partition}")


def _rewrite_by_path(result, path: str) -> dict:
    for item in result["files"]["rewritten"]:
        if item["path"] == path:
            return item
    raise AssertionError(f"no rewrite at {path}")


def test_classification_by_path_and_hash_without_delete_plus_add():
    old = _manifest([
        _entry("dt=a/keep.csv", 10, _sha("keep"), {"dt": "a"}),
        _entry("dt/a/rewrite-reuse.csv", 20, _sha("old-reuse"), {"dt": "a"}),
        _entry("dt/a/rewrite-drift.csv", 30, _sha("old-drift"), {"dt": "a"}),
        _entry("dt/a/rewrite-gone.csv", 40, _sha("old-gone"), {"dt": "a"}),
        _entry("dt/a/gone.csv", 50, _sha("gone"), {"dt": "a"}),
        _entry("dt=b/keep.csv", 60, _sha("keep-b"), {"dt": "b"}),
    ])
    new = _manifest([
        _entry("dt=a/keep.csv", 10, _sha("keep"), {"dt": "a"}),
        _entry("dt/a/rewrite-reuse.csv", 25, _sha("new-reuse"), {"dt": "a"}),
        _entry("dt/a/renamed-copy.csv", 20, _sha("old-reuse"), {"dt": "a"}),
        _entry("dt/a/rewrite-drift.csv", 33, _sha("new-drift"), {"dt": "a"}),
        _entry("dt=b/moved-copy.csv", 30, _sha("old-drift"), {"dt": "b"}),
        _entry("dt/a/rewrite-gone.csv", 44, _sha("new-gone"), {"dt": "a"}),
        _entry("dt/a/brand-new.csv", 70, _sha("brand-new"), {"dt": "a"}),
        _entry("dt=b/keep.csv", 60, _sha("keep-b"), {"dt": "b"}),
    ])
    result = compute_file_diff("orders", old, new)

    # Rewrites stay rewrites: they are never split into delete + add.
    assert result["counts"] == {
        "added": 3,
        "deleted": 1,
        "rewritten": 3,
        "unchanged": 2,
    }
    assert result["bytes"] == {
        "added": 120,
        "deleted": 50,
        "rewritten_old": 90,
        "rewritten_new": 102,
        "unchanged": 70,
    }
    assert [item["path"] for item in result["files"]["added"]] == [
        "dt/a/brand-new.csv",
        "dt/a/renamed-copy.csv",
        "dt=b/moved-copy.csv",
    ]
    assert [item["path"] for item in result["files"]["deleted"]] == ["dt/a/gone.csv"]

    reuse_ok = _rewrite_by_path(result, "dt/a/rewrite-reuse.csv")["reuse"]
    assert reuse_ok["status"] == "reusable"
    assert reuse_ok["reusable_bytes"] == 20
    assert reuse_ok["matched_path"] == "dt/a/renamed-copy.csv"
    assert reuse_ok["matched_partition"] == {"dt": "a"}

    drift = _rewrite_by_path(result, "dt/a/rewrite-drift.csv")["reuse"]
    assert drift["status"] == "partition_mismatch"
    assert drift["reusable_bytes"] == 0
    assert drift["matched_path"] == "dt=b/moved-copy.csv"
    assert drift["matched_partition"] == {"dt": "b"}
    assert drift["content_locations"] == [
        {"path": "dt=b/moved-copy.csv", "partition": {"dt": "b"}}
    ]

    gone = _rewrite_by_path(result, "dt/a/rewrite-gone.csv")["reuse"]
    assert gone["status"] == "content_absent"
    assert gone["reusable_bytes"] == 0
    assert gone["content_locations"] == []

    assert result["reuse"] == {
        "reusable_files": 1,
        "reusable_bytes": 20,
        "non_reusable_files": 2,
        "non_reusable_bytes": 70,
        "reasons": {
            "partition_mismatch": {"files": 1, "bytes": 30},
            "content_absent": {"files": 1, "bytes": 40},
        },
    }


def test_partition_rollup_totals_match_file_level_lists():
    old = _manifest([
        _entry("dt=a/keep.csv", 10, _sha("keep"), {"dt": "a"}),
        _entry("dt/a/rewrite-reuse.csv", 20, _sha("old-reuse"), {"dt": "a"}),
        _entry("dt/a/rewrite-drift.csv", 30, _sha("old-drift"), {"dt": "a"}),
        _entry("dt/a/rewrite-gone.csv", 40, _sha("old-gone"), {"dt": "a"}),
        _entry("dt/a/gone.csv", 50, _sha("gone"), {"dt": "a"}),
        _entry("dt=b/keep.csv", 60, _sha("keep-b"), {"dt": "b"}),
    ])
    new = _manifest([
        _entry("dt=a/keep.csv", 10, _sha("keep"), {"dt": "a"}),
        _entry("dt/a/rewrite-reuse.csv", 25, _sha("new-reuse"), {"dt": "a"}),
        _entry("dt/a/renamed-copy.csv", 20, _sha("old-reuse"), {"dt": "a"}),
        _entry("dt/a/rewrite-drift.csv", 33, _sha("new-drift"), {"dt": "a"}),
        _entry("dt=b/moved-copy.csv", 30, _sha("old-drift"), {"dt": "b"}),
        _entry("dt/a/rewrite-gone.csv", 44, _sha("new-gone"), {"dt": "a"}),
        _entry("dt/a/brand-new.csv", 70, _sha("brand-new"), {"dt": "a"}),
        _entry("dt=b/keep.csv", 60, _sha("keep-b"), {"dt": "b"}),
    ])
    result = compute_file_diff("orders", old, new)
    assert result["partition_attribution"]["status"] == "ok"

    rolled_counts = {name: 0 for name in ("added", "deleted", "rewritten", "unchanged")}
    rolled_bytes = {
        name: 0
        for name in ("added", "deleted", "rewritten_old", "rewritten_new", "unchanged")
    }
    for bucket in result["partitions"]:
        for name, value in bucket["counts"].items():
            rolled_counts[name] += value
        for name, value in bucket["bytes"].items():
            rolled_bytes[name] += value
    assert rolled_counts == result["counts"]
    assert rolled_bytes == result["bytes"]

    bucket_a = _rollup(result, {"dt": "a"})
    assert bucket_a["counts"] == {
        "added": 2,
        "deleted": 1,
        "rewritten": 3,
        "unchanged": 1,
    }
    assert bucket_a["bytes"] == {
        "added": 90,
        "deleted": 50,
        "rewritten_old": 90,
        "rewritten_new": 102,
        "unchanged": 10,
    }
    bucket_b = _rollup(result, {"dt": "b"})
    assert bucket_b["counts"] == {"added": 1, "deleted": 0, "rewritten": 0, "unchanged": 1}
    assert bucket_b["bytes"] == {
        "added": 30,
        "deleted": 0,
        "rewritten_old": 0,
        "rewritten_new": 0,
        "unchanged": 60,
    }

    # Whole-snapshot byte reconciliation against each manifest total.
    assert old["total_bytes"] == (
        result["bytes"]["deleted"]
        + result["bytes"]["rewritten_old"]
        + result["bytes"]["unchanged"]
    )
    assert new["total_bytes"] == (
        result["bytes"]["added"]
        + result["bytes"]["rewritten_new"]
        + result["bytes"]["unchanged"]
    )


def test_same_snapshot_is_zero_change():
    manifest = _manifest([
        _entry("dt=a/part-00000.csv", 10, _sha("a"), {"dt": "a"}),
        _entry("dt=b/part-00000.csv", 20, _sha("b"), {"dt": "b"}),
    ])
    result = compute_file_diff("orders", manifest, manifest)
    assert result["counts"] == {"added": 0, "deleted": 0, "rewritten": 0, "unchanged": 2}
    assert result["bytes"] == {
        "added": 0,
        "deleted": 0,
        "rewritten_old": 0,
        "rewritten_new": 0,
        "unchanged": 30,
    }
    assert result["reuse"] == {
        "reusable_files": 0,
        "reusable_bytes": 0,
        "non_reusable_files": 0,
        "non_reusable_bytes": 0,
        "reasons": {
            "partition_mismatch": {"files": 0, "bytes": 0},
            "content_absent": {"files": 0, "bytes": 0},
        },
    }
    assert result["partition_attribution"]["status"] == "ok"
    assert result["partition_attribution"]["unattributable"] == []


def test_partition_column_drift_is_structured_not_an_error():
    old = _manifest(
        [
            _entry("dt=a/part-00000.csv", 10, _sha("old-a"), {"dt": "a"}),
            _entry("dt=b/part-00000.csv", 20, _sha("old-b"), {"dt": "b"}),
        ],
        partition_columns=("dt",),
    )
    new = _manifest(
        [
            _entry(
                "region=x/part-00000.csv",
                11,
                _sha("new-x"),
                {"region": "x"},
            ),
            _entry(
                "region=y/part-00000.csv",
                20,
                _sha("old-b"),
                {"region": "y"},
            ),
        ],
        partition_columns=("region",),
    )
    # Must not raise even though partition columns no longer line up.
    result = compute_file_diff("orders", old, new)
    attribution = result["partition_attribution"]
    assert attribution["status"] == "inconsistent"
    assert attribution["from_partition_columns"] == ["dt"]
    assert attribution["to_partition_columns"] == ["region"]
    assert attribution["unattributable"] == [
        {
            "type": "partition_column_drift",
            "from_partition_columns": ["dt"],
            "to_partition_columns": ["region"],
            "only_in_from": ["dt"],
            "only_in_to": ["region"],
            "message": (
                "snapshots use different partition columns; per-partition rollup "
                "cannot be aligned, file-level attribution is still provided"
            ),
        }
    ]
    # No per-partition rollup, but file-level attribution still works.
    assert result["partitions"] == []
    assert result["counts"] == {"added": 2, "deleted": 2, "rewritten": 0, "unchanged": 0}
    assert result["bytes"] == {
        "added": 31,
        "deleted": 30,
        "rewritten_old": 0,
        "rewritten_new": 0,
        "unchanged": 0,
    }


def test_annotation_drift_on_rewrite_is_recorded_per_file():
    old = _manifest([
        _entry("dt=a/moved.csv", 30, _sha("old-drift"), {"dt": "a"}),
    ])
    new = _manifest([
        _entry("dt=a/moved.csv", 33, _sha("new-drift"), {"dt": "a"}),
        _entry("dt=b/copy.csv", 30, _sha("old-drift"), {"dt": "b"}),
    ])
    result = compute_file_diff("orders", old, new)
    assert result["partition_attribution"]["status"] == "ok"
    rewrite = result["files"]["rewritten"][0]
    assert rewrite["reuse"]["status"] == "partition_mismatch"
    assert rewrite["reuse"]["reusable_bytes"] == 0
    # Same dt column, so the drift is a per-file annotation, not column drift.
    assert result["partition_attribution"]["unattributable"] == []


def test_output_is_byte_for_byte_deterministic():
    old = _manifest([
        _entry("dt=a/rewrite.csv", 20, _sha("old"), {"dt": "a"}),
        _entry("dt=b/keep.csv", 60, _sha("keep"), {"dt": "b"}),
    ])
    new = _manifest([
        _entry("dt=b/keep.csv", 60, _sha("keep"), {"dt": "b"}),
        _entry("dt/a/rewrite.csv", 22, _sha("new"), {"dt": "a"}),
        _entry("dt/a/copy.csv", 20, _sha("old"), {"dt": "a"}),
    ])
    first = json.dumps(compute_file_diff("orders", old, new), sort_keys=True)
    second = json.dumps(compute_file_diff("orders", old, new), sort_keys=True)
    assert first == second


def test_file_diff_versions_resolves_ledger_entries(make_config, write_table):
    config = make_config()
    schema = {"order_id": "int", "amount": "float", "dt": "str"}
    v1 = [
        {"order_id": 1, "amount": 1.0, "dt": "2024-02-01"},
        {"order_id": 2, "amount": 2.0, "dt": "2024-02-02"},
    ]
    v2 = [
        {"order_id": 1, "amount": 1.5, "dt": "2024-02-01"},
        {"order_id": 2, "amount": 2.0, "dt": "2024-02-02"},
        {"order_id": 3, "amount": 3.0, "dt": "2024-02-03"},
    ]

    def group(rows):
        grouped = {}
        for row in rows:
            grouped.setdefault(row["dt"], []).append(row)
        return grouped

    write_table(config.lake_root / "orders", group(v1), schema)
    first = create_snapshot(config, "orders", clock=lambda: "2025-01-01T00:00:00Z")
    write_table(config.lake_root / "orders", group(v2), schema)
    second = create_snapshot(config, "orders", clock=lambda: "2025-01-02T00:00:00Z")

    ledger = Ledger(config.ledger_dir)
    result = file_diff_versions(ledger, "orders", 1, 2)
    assert result["from"]["snapshot_id"] == first["snapshot_id"]
    assert result["to"]["snapshot_id"] == second["snapshot_id"]
    # Same two relative paths: one rewritten, one unchanged; plus one new file.
    assert result["counts"]["unchanged"] == 1
    assert result["counts"]["rewritten"] == 1
    assert result["counts"]["added"] == 1
    assert result["counts"]["deleted"] == 0

    zero = file_diff_versions(ledger, "orders", "v2", "latest")
    assert zero["counts"] == {"added": 0, "deleted": 0, "rewritten": 0, "unchanged": 3}


def test_file_diff_cli_flow_with_generated_fixtures(make_config, capsys):
    from lakeback.cli import main
    from lakeback.fixtures.generate import generate_fixtures

    config = make_config()
    generate_fixtures(config)

    def run(*arguments):
        code = main(["--config", str(config.path), *arguments, "--json"])
        captured = capsys.readouterr()
        return code, json.loads(captured.out), captured.err

    code, first, _ = run("snapshot", "--table", "orders")
    assert code == 0
    shutil.rmtree(config.lake_root / "orders")
    shutil.copytree(config.fixtures_dir / "before" / "orders", config.lake_root / "orders")
    code, second, _ = run("snapshot", "--table", "orders")
    assert code == 0
    assert first["snapshot_id"] != second["snapshot_id"]

    code, result, err = run("file-diff", "--table", "orders", "--from", "1", "--to", "2")
    assert code == 0
    assert err == ""
    assert result["table"] == "orders"
    assert result["from"]["version"] == 1
    assert result["to"]["version"] == 2
    rolled = {name: 0 for name in ("added", "deleted", "rewritten", "unchanged")}
    for bucket in result["partitions"]:
        for name, value in bucket["counts"].items():
            rolled[name] += value
    assert rolled == result["counts"]

    code, zero, _ = run("file-diff", "--table", "orders", "--from", "2", "--to", "2")
    assert code == 0
    assert zero["counts"] == {
        "added": 0,
        "deleted": 0,
        "rewritten": 0,
        "unchanged": second["file_count"],
    }
