from __future__ import annotations

import json
import re

import pytest

from lakeback._util import REPORT_RE
from lakeback.backfill.auditor import run_audit
from lakeback.backfill.report import load_report, render_markdown
from lakeback.errors import NotFoundError, RequestError

SCHEMA = {"order_id": "int", "amount": "float", "status": "str", "dt": "str"}
ROWS = [
    {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
    {"order_id": 2, "amount": 20.0, "status": "paid", "dt": "2024-02-01"},
    {"order_id": 3, "amount": 30.0, "status": "pending", "dt": "2024-02-02"},
]


def _group(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["dt"], []).append(row)
    return grouped


def _roots(config, write_table, before_rows, after_rows, after_schema=None):
    before = write_table(config.project_root / "before" / "orders", _group(before_rows), SCHEMA)
    after = write_table(
        config.project_root / "after" / "orders",
        _group(after_rows),
        after_schema or SCHEMA,
    )
    return before, after


def test_consistent_audit(make_config, write_table):
    config = make_config()
    before, after = _roots(config, write_table, ROWS, ROWS)
    report = run_audit(config, "orders", before, after)
    assert report["status"] == "consistent"
    assert all(check["status"] == "pass" for check in report["checks"])
    assert report["counts"]["modified_rows"] == 0
    assert report["counts"]["missing_in_after"] == 0
    assert report["residual"]["missing_in_after"] == []
    assert REPORT_RE.fullmatch(report["report_id"])
    assert report["checks"][0]["name"] == "schema"
    assert report["checks"][0]["evidence"]["differences"]["missing_fields"] == []
    assert report["artifacts"]["json_report"].endswith(".json")
    assert report["artifacts"]["markdown_report"].endswith(".md")


def test_residual_diff_audit(make_config, write_table):
    config = make_config()
    after_rows = [
        {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
        {"order_id": 2, "amount": 25.5, "status": "refunded", "dt": "2024-02-01"},
        {"order_id": 5, "amount": 50.0, "status": "new", "dt": "2024-02-02"},
    ]
    before, after = _roots(config, write_table, ROWS, after_rows)
    report = run_audit(config, "orders", before, after)
    assert report["status"] == "residual_diff"
    assert report["counts"]["missing_in_after"] == 1
    assert report["counts"]["extra_in_after"] == 1
    assert report["counts"]["modified_rows"] == 1
    assert report["residual"]["missing_in_after"] == [3]
    assert report["residual"]["extra_in_after"] == [5]
    changes = {item["name"]: item for item in report["residual"]["modified_rows"][0]["fields"]}
    assert changes["amount"] == {"name": "amount", "before": 20.0, "after": 25.5}
    assert changes["status"]["after"] == "refunded"
    check_map = {check["name"]: check["status"] for check in report["checks"]}
    assert check_map["schema"] == "pass"
    assert check_map["partition_structure"] == "pass"
    assert check_map["primary_key_set"] == "fail"
    assert check_map["partition_checksums"] == "fail"
    assert report["partition_checksum_mismatches"]


def test_structural_mismatch_from_schema(make_config, write_table):
    config = make_config()
    after_schema = {"order_id": "int", "amount": "int", "status": "str", "dt": "str"}
    after_rows = [{**row, "amount": int(row["amount"])} for row in ROWS]
    before, after = _roots(config, write_table, ROWS, after_rows, after_schema)
    report = run_audit(config, "orders", before, after)
    assert report["status"] == "structural_mismatch"
    schema_check = next(check for check in report["checks"] if check["name"] == "schema")
    assert schema_check["status"] == "fail"
    assert schema_check["evidence"]["differences"]["type_changes"]["amount"] == {
        "before": "float", "after": "int"
    }


def test_structural_mismatch_from_missing_partition(make_config, write_table):
    config = make_config()
    after_rows = [row for row in ROWS if row["dt"] == "2024-02-01"]
    before, after = _roots(config, write_table, ROWS, after_rows)
    report = run_audit(config, "orders", before, after)
    assert report["status"] == "structural_mismatch"
    partition_check = next(
        check for check in report["checks"] if check["name"] == "partition_structure"
    )
    assert partition_check["status"] == "fail"
    assert report["residual"]["missing_partitions"] == [{"dt": "2024-02-02"}]


def test_report_is_deterministic_and_loadable(make_config, write_table):
    config = make_config()
    before, after = _roots(config, write_table, ROWS, ROWS)
    first = run_audit(config, "orders", before, after)
    second = run_audit(config, "orders", before, after)
    assert first["report_id"] == second["report_id"]
    loaded = load_report(config.reports_dir, first["report_id"])
    assert loaded["status"] == "consistent"
    assert loaded["checks"] == first["checks"]
    markdown = (config.reports_dir / f"{first['report_id']}.md").read_text(encoding="utf-8")
    assert "**consistent**" in markdown
    assert render_markdown(loaded).startswith("# Backfill Audit Report: orders")
    json_payload = json.loads((config.reports_dir / f"{first['report_id']}.json").read_text())
    assert json_payload["report_id"] == first["report_id"]


def test_load_report_validation(make_config):
    config = make_config()
    with pytest.raises(RequestError, match="report_id"):
        load_report(config.reports_dir, "../../secret")
    with pytest.raises(NotFoundError, match="not found"):
        load_report(config.reports_dir, "rpt-" + "a" * 16)


def test_audit_requires_existing_roots(make_config):
    config = make_config()
    with pytest.raises(NotFoundError, match="before table directory"):
        run_audit(config, "orders", config.project_root / "missing", config.project_root)
