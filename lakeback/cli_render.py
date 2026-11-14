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
    return "\n".join([
        f"snapshot {manifest['snapshot_id']}",
        f"  table: {manifest['table']}  version: {manifest['version']}",
        f"  parent: {manifest['parent_id']}",
        f"  created_at: {manifest['created_at']}",
        f"  rows/files/bytes: {manifest['row_count']} / "
        f"{manifest['file_count']} / {manifest['total_bytes']}",
        f"  partitions: {json.dumps(manifest['partitions'], sort_keys=True)}",
    ])


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


def render_referential(result: dict) -> str:
    counts = result["counts"]
    lines = [
        f"referential audit status={result['status']} null_policy={result['null_policy']}",
        f"  fact      {result['fact']['table']} v{result['fact']['version']} "
        f"{result['fact']['snapshot_id']}",
        f"  dimension {result['dimension']['table']} v{result['dimension']['version']} "
        f"{result['dimension']['snapshot_id']}",
        "  key: " + ", ".join(
            f"{pair['fact_column']}={pair['dimension_column']} "
            f"({pair['compare_type']}: {pair['fact_type']}/{pair['dimension_type']})"
            for pair in result["foreign_key"]
        ),
        f"  rows={counts['fact_rows']} checked={counts['checked']} "
        f"matched={counts['matched']} orphans={counts['orphans']} "
        f"null_skipped={counts['null_skipped']}",
        f"  parent-key duplicates: {counts['parent_key_duplicates']} key(s) / "
        f"{counts['parent_key_duplicate_rows']} row(s); "
        f"ambiguous fact matches={counts['ambiguous_parent_matches']}",
    ]
    for item in result["orphans"][:5]:
        lines.append(f"  ! row {item['row_index']} " + json.dumps(
            item["foreign_key"], sort_keys=True, ensure_ascii=False
        ))
    if counts["orphans"] > 5:
        lines.append(f"  ... {counts['orphans'] - 5} more orphans; use --json for all")
    for item in result["parent_key_duplicates"][:5]:
        lines.append(
            f"  x duplicate parent key {json.dumps(item['key'], sort_keys=True)} "
            f"rows={item['row_count']}"
        )
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
