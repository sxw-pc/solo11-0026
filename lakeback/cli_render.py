"""Human-readable renderers kept separate from argument handling."""

from __future__ import annotations

import json


def _json(value) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False)


def render_generated(result: dict) -> str:
    lines = [
        f"generated fixtures with backend={result['backend']} seed={result['seed']}",
        f"before: {result['paths']['before']}",
        f"after:  {result['paths']['after']}",
        f"lake:   {result['paths']['lake']}",
    ]
    for table, info in result["tables"].items():
        lines.append(
            f"  {table}: before={info['before_rows']} after={info['after_rows']}"
        )
    for name, digest in result["digests"].items():
        lines.append(f"  digest {name}: {digest}")
    return "\n".join(lines)


def render_snapshot(manifest: dict) -> str:
    lines = [
        f"snapshot {manifest['snapshot_id']}",
        f"  table: {manifest['table']}  version: {manifest['version']}",
        f"  parent: {manifest['parent_id']}",
        f"  created_at: {manifest['created_at']}",
        f"  rows/files/bytes: {manifest['row_count']} / "
        f"{manifest['file_count']} / {manifest['total_bytes']}",
        f"  partitions: {json.dumps(manifest['partitions'], sort_keys=True)}",
    ]
    lineage = manifest.get("lineage")
    if lineage is not None:
        lines.append(f"  lineage sources: {_source_list(lineage['sources'])}")
    return "\n".join(lines)


def _source_list(sources: list[dict]) -> str:
    if not sources:
        return "(none: source table)"
    return ", ".join(f"{source['table']}={source['version']}" for source in sources)


def render_lineage_registered(result: dict) -> str:
    state = "registered" if result["created"] else "already registered (unchanged)"
    lines = [
        f"lineage {state} for {result['snapshot_id']}",
        f"  table: {result['table']}  version: {result['version']}",
        f"  sources: {_source_list(result['sources'])}",
    ]
    if result["unresolved"]:
        lines.append(f"  unresolved (not in ledger): {_source_list(result['unresolved'])}")
    return "\n".join(lines)


def _render_lineage_walk(result: dict) -> str:
    root = result["root"]
    lines = [f"{result['direction']} of {root['table']} v{root['version']} "
             f"({root['snapshot_id']})"]
    if not result["entries"]:
        lines.append("  (no linked snapshots)")
    for entry in result["entries"]:
        marker = "" if entry["status"] == "resolved" else "  [dangling: not in ledger]"
        lines.append(
            f"  depth {entry['depth']}: {entry['table']} v{entry['version']} "
            f"{entry['snapshot_id'] or '-'}{marker}"
        )
    if result["truncated"]:
        lines.append(
            f"  ... truncated: showing {result['entry_count']} of "
            f"{result['total_count']}; raise --limit to see more"
        )
    return "\n".join(lines)


def render_lineage_upstream(result: dict) -> str:
    return _render_lineage_walk(result)


def render_lineage_downstream(result: dict) -> str:
    return _render_lineage_walk(result)


def render_travel(result: dict) -> str:
    snapshot = result["snapshot"]
    lines = [
        f"table={result['table']} at={result['at']}",
        f"  resolved version={snapshot['version']} snapshot={snapshot['snapshot_id']}",
        f"  created_at={snapshot['created_at']} partitions={snapshot['partitions']}",
        f"  rows={result['total_rows']} returned={result['returned']} "
        f"offset={result['offset']} limit={result['limit']}",
        f"  filters={result['partition_filters']}",
    ]
    for row in result["rows"][:10]:
        lines.append("  " + json.dumps(row, sort_keys=False, ensure_ascii=False))
    if result["returned"] > 10:
        lines.append(f"  ... {result['returned'] - 10} more rows; use --json for all")
    return "\n".join(lines)


def render_diff(result: dict) -> str:
    counts = result["counts"]
    lines = [
        f"diff table={result['table']} key={result['key']}",
        f"  from v{result['from']['version']} {result['from']['snapshot_id']}",
        f"  to   v{result['to']['version']} {result['to']['snapshot_id']}",
        f"  added={counts['added']} removed={counts['removed']} "
        f"modified={counts['modified']} unchanged={counts['unchanged']}",
    ]
    for row in result["added"][:5]:
        lines.append("  + " + json.dumps(row, sort_keys=True))
    for row in result["removed"][:5]:
        lines.append("  - " + json.dumps(row, sort_keys=True))
    for item in result["modified"][:5]:
        fields = ", ".join(change["name"] for change in item["fields"])
        lines.append(f"  ~ key={item['key']} fields={fields}")
    total = len(result["added"]) + len(result["removed"]) + len(result["modified"])
    if total > 15:
        lines.append(f"  ... {total - 15} more changes; use --json for detail")
    return "\n".join(lines)


def render_audit(report: dict) -> str:
    counts = report["counts"]
    lines = [
        f"backfill audit {report['report_id']}",
        f"  table={report['table']} status={report['status']}",
        f"  {report['summary']}",
        f"  before={report['before_root']}",
        f"  after ={report['after_root']}",
        f"  rows {counts['before_rows']} -> {counts['after_rows']}; "
        f"keys {counts['before_keys']} -> {counts['after_keys']}",
        f"  missing/extra/modified: {counts['missing_in_after']}/"
        f"{counts['extra_in_after']}/{counts['modified_rows']}",
        "  checks:",
    ]
    for check in report["checks"]:
        lines.append(f"    {check['name']}: {check['status']}")
    lines.append(f"  json report: {report['artifacts']['json_report']}")
    lines.append(f"  markdown:    {report['artifacts']['markdown_report']}")
    return "\n".join(lines)


def render_doctor(result: dict) -> str:
    lines = [
        f"LakeBack environment check ({result['version']})",
        f"  python: {result['python']}",
        f"  pyarrow: {result['pyarrow'] or 'not installed'}",
        f"  backends: {result['backends']}",
        f"  configured backend: {result['configured_backend']}",
        f"  config: {result['config']}",
        f"  seed: {result['seed_path']} (exists={result['seed_exists']})",
        f"  service bind: {result['service']['host']}:{result['service']['port']}",
        f"  ledger: {result['ledger']}",
        f"  directories: {result['directories']}",
        f"  result: {'OK' if result['ok'] else 'ATTENTION REQUIRED'}",
    ]
    return "\n".join(lines)


def render_selftest(result: dict) -> str:
    lines = [f"selftest backend={result['backend']} tempdir={result['temporary']}"]
    for name, status in result["checks"].items():
        lines.append(f"  {name}: {status}")
    lines.append(f"  result: {'OK' if result['ok'] else 'FAILED'}")
    return "\n".join(lines)
