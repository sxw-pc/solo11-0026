"""Read-only traversal of the snapshot lineage graph (upstream and downstream)."""

from __future__ import annotations

from lakeback.errors import LineageCycleError, NotFoundError


def _labels(ledger) -> dict:
    return {
        manifest["snapshot_id"]: (manifest["table"], manifest["version"])
        for manifest in ledger.snapshots()
    }


def _resolved_edges(ledger, store) -> tuple[dict, dict, dict]:
    """Split every declared source into resolved edges and dangling references."""
    labels = _labels(ledger)
    edges: dict[str, list[str]] = {}
    dangling: dict[str, list[dict]] = {}
    for entry in store.records():
        resolved: list[str] = []
        missing: list[dict] = []
        for source in entry["sources"]:
            try:
                target = ledger.get_version(source["table"], source["version"])
                resolved.append(target["snapshot_id"])
            except NotFoundError:
                missing.append({"table": source["table"], "version": source["version"]})
        edges[entry["snapshot_id"]] = sorted(resolved, key=lambda sid: labels[sid])
        dangling[entry["snapshot_id"]] = missing
    return edges, dangling, labels


def _check_cycles(root_id: str, neighbours, labels: dict) -> None:
    """Raise LineageCycleError if any cycle is reachable from root_id."""
    color = {root_id: 1}  # 1 = on the DFS path, 2 = fully explored
    path = [root_id]
    stack = [(root_id, iter(neighbours(root_id)))]
    while stack:
        node, children = stack[-1]
        descended = False
        for child in children:
            state = color.get(child, 0)
            if state == 0:
                color[child] = 1
                path.append(child)
                stack.append((child, iter(neighbours(child))))
                descended = True
                break
            if state == 1:
                cycle = path[path.index(child):] + [child]
                message = "lineage cycle detected: " + " -> ".join(
                    f"{sid} ({labels[sid][0]} v{labels[sid][1]})" for sid in cycle
                )
                raise LineageCycleError(message, cycle=cycle)
        if not descended:
            color[node] = 2
            stack.pop()
            path.pop()


def _walk(root_id: str, adjacency) -> tuple[dict, dict]:
    """Breadth-first layering; returns (depth, via) for every reachable node."""
    depth = {root_id: 0}
    via: dict[str, set] = {}
    queue = [root_id]
    while queue:
        node = queue.pop(0)
        for child in adjacency(node):
            via.setdefault(child, set()).add(node)
            if child not in depth:
                depth[child] = depth[node] + 1
                queue.append(child)
    return depth, via


def _root(manifest: dict, declared: bool) -> dict:
    return {
        "table": manifest["table"],
        "version": manifest["version"],
        "snapshot_id": manifest["snapshot_id"],
        "declared": declared,
    }


def _resolved_entry(snapshot_id: str, depth: int, labels: dict, edges: dict, via: dict) -> dict:
    table, version = labels[snapshot_id]
    return {
        "depth": depth,
        "table": table,
        "version": version,
        "snapshot_id": snapshot_id,
        "status": "resolved",
        "declared": snapshot_id in edges,
        "via": sorted(via.get(snapshot_id, set())),
    }


def _sorted(entries: list[dict]) -> list[dict]:
    return sorted(entries, key=lambda item: (item["depth"], item["table"], item["version"]))


def upstream(ledger, store, snapshot_id: str) -> dict:
    """List every upstream snapshot the given snapshot transitively read."""
    root = ledger.get_snapshot(snapshot_id)
    edges, dangling, labels = _resolved_edges(ledger, store)
    _check_cycles(snapshot_id, lambda sid: edges.get(sid, []), labels)
    depth, via = _walk(snapshot_id, lambda sid: edges.get(sid, []))
    entries = [
        _resolved_entry(sid, level, labels, edges, via)
        for sid, level in depth.items()
        if sid != snapshot_id
    ]
    leaves: dict[tuple, dict] = {}
    for sid, level in depth.items():
        for ref in dangling.get(sid, []):
            key = (ref["table"], ref["version"])
            leaf = leaves.setdefault(key, {"depth": level + 1, "via": set()})
            leaf["depth"] = min(leaf["depth"], level + 1)
            leaf["via"].add(sid)
    for (table, version), leaf in leaves.items():
        entries.append({
            "depth": leaf["depth"],
            "table": table,
            "version": version,
            "snapshot_id": None,
            "status": "dangling",
            "declared": False,
            "via": sorted(leaf["via"]),
        })
    entries = _sorted(entries)
    return {
        "direction": "upstream",
        "root": _root(root, declared=snapshot_id in edges),
        "entries": entries,
        "entry_count": len(entries),
        "total_count": len(entries),
        "truncated": False,
        "limit": None,
    }


def downstream(ledger, store, snapshot_id: str, limit: int | None = None) -> dict:
    """List every snapshot that directly or transitively depends on the given one."""
    root = ledger.get_snapshot(snapshot_id)
    edges, _, labels = _resolved_edges(ledger, store)
    reverse: dict[str, list[str]] = {}
    for dependent, sources in edges.items():
        for source_id in sources:
            reverse.setdefault(source_id, []).append(dependent)
    for dependents in reverse.values():
        dependents.sort(key=lambda sid: labels[sid])
    _check_cycles(snapshot_id, lambda sid: reverse.get(sid, []), labels)
    depth, via = _walk(snapshot_id, lambda sid: reverse.get(sid, []))
    entries = _sorted([
        _resolved_entry(sid, level, labels, edges, via)
        for sid, level in depth.items()
        if sid != snapshot_id
    ])
    total = len(entries)
    truncated = limit is not None and total > limit
    shown = entries[:limit] if limit is not None else entries
    return {
        "direction": "downstream",
        "root": _root(root, declared=snapshot_id in edges),
        "entries": shown,
        "entry_count": len(shown),
        "total_count": total,
        "truncated": truncated,
        "limit": limit,
    }
