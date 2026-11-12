"""Persist audit reports as stable JSON plus a compact Markdown summary."""

from __future__ import annotations

import json
from pathlib import Path

from lakeback._util import REPORT_RE
from lakeback.errors import NotFoundError, RequestError

STATUS_TEXT = {
    "consistent": "no residual differences were found",
    "residual_diff": "data differences remain after the backfill",
    "structural_mismatch": "schema or partition structure differs",
}


def _short(value, limit: int = 80) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def render_markdown(report: dict) -> str:
    lines = [
        f"# Backfill Audit Report: {report['table']}",
        "",
        f"- Report ID: `{report['report_id']}`",
        f"- Status: **{report['status']}**",
        f"- Generated at: `{report['generated_at']}`",
        f"- Primary key: `{report['key']}`",
        f"- Before: `{report['before_root']}`",
        f"- After: `{report['after_root']}`",
        "",
        f"> {STATUS_TEXT.get(report['status'], report['status'])}",
        "",
        "## Checks",
        "",
        "| Check | Result | Evidence |",
        "| --- | --- | --- |",
    ]
    for check in report["checks"]:
        lines.append(
            f"| `{check['name']}` | **{check['status']}** | "
            f"`{_short(check['evidence']).replace('|', '/')}` |"
        )
    counts = report["counts"]
    lines.extend([
        "",
        "## Counts",
        "",
        f"- rows: {counts['before_rows']} -> {counts['after_rows']}",
        f"- keys: {counts['before_keys']} -> {counts['after_keys']}",
        f"- missing in after: {counts['missing_in_after']}",
        f"- extra in after: {counts['extra_in_after']}",
        f"- modified rows: {counts['modified_rows']}",
        f"- unchanged rows: {counts['unchanged_rows']}",
        "",
        "## Residual Detail",
        "",
    ])
    residual = report["residual"]
    if not residual["missing_in_after"] and not residual["extra_in_after"] and not residual["modified_rows"]:
        lines.append("No row-level residual differences.")
    else:
        if residual["missing_in_after"]:
            lines.append(
                "- Missing in after: "
                + ", ".join(str(value) for value in residual["missing_in_after"][:20])
            )
        if residual["extra_in_after"]:
            lines.append(
                "- Extra in after: "
                + ", ".join(str(value) for value in residual["extra_in_after"][:20])
            )
        for item in residual["modified_rows"][:20]:
            fields = ", ".join(change["name"] for change in item["fields"])
            lines.append(f"- Modified key `{item['key']}` fields: {fields}")
        omitted = (
            len(residual["missing_in_after"]) + len(residual["extra_in_after"])
            + len(residual["modified_rows"]) - 40
        )
        if omitted > 0:
            lines.append(f"- ... {omitted} more row details are in the JSON report")
    if report["partition_checksum_mismatches"]:
        lines.extend(["", "## Partition Checksum Mismatches", ""])
        for item in report["partition_checksum_mismatches"]:
            lines.append(
                f"- `{_short(item['partition'])}`: fields "
                + ", ".join(item["mismatched_fields"])
            )
    lines.extend([
        "",
        "## Reproduction",
        "",
        f"`lakeback backfill-audit --table {report['table']} "
        f"--before {report['before_root']} --after {report['after_root']} --json`",
        "",
    ])
    return "\n".join(lines)


def write_audit_report(reports_dir: Path, report: dict) -> tuple[Path, Path]:
    reports_dir = Path(reports_dir).resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / f"{report['report_id']}.json"
    markdown_path = reports_dir / f"{report['report_id']}.md"
    payload = dict(report)
    payload["artifacts"] = {
        "json_report": str(json_path),
        "markdown_report": str(markdown_path),
    }
    try:
        json_path.write_text(
            json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        markdown_path.write_text(render_markdown(payload), encoding="utf-8")
    except OSError as exc:
        raise RequestError(f"cannot write audit report: {exc}") from exc
    return json_path, markdown_path


def load_report(reports_dir: Path, report_id: str) -> dict:
    if not REPORT_RE.fullmatch(report_id):
        raise RequestError("report_id must look like rpt-<16 lowercase hex characters>")
    path = Path(reports_dir).resolve() / f"{report_id}.json"
    if not path.is_file():
        raise NotFoundError(f"backfill report not found: {report_id}")
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RequestError(f"backfill report is unreadable: {report_id}") from exc
    if report.get("report_id") != report_id:
        raise RequestError(f"backfill report id mismatch: {report_id}")
    return report
