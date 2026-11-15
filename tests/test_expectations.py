from __future__ import annotations

import json

import pytest

from lakeback.backfill.auditor import run_audit
from lakeback.backfill.expectations import load_expected_changes
from lakeback.cli import main
from lakeback.errors import ConfigError

SCHEMA = {"order_id": "int", "amount": "float", "status": "str", "dt": "str"}
BASE_ROWS = [
    {"order_id": 1, "amount": 10.0, "status": "paid", "dt": "2024-02-01"},
    {"order_id": 2, "amount": 20.0, "status": "paid", "dt": "2024-02-01"},
    {"order_id": 3, "amount": 30.0, "status": "pending", "dt": "2024-02-02"},
]


def _group(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["dt"], []).append(row)
    return grouped


def _write_pair(config, write_table, after_rows, before_rows=None):
    before = write_table(
        config.project_root / "before" / "orders",
        _group(before_rows or BASE_ROWS), SCHEMA,
    )
    after = write_table(
        config.project_root / "after" / "orders", _group(after_rows), SCHEMA
    )
    return before, after


def _change(rows, key, **updates):
    return [{**row, **updates} if row["order_id"] == key else row for row in rows]


def _declaration(config, rules, name="expected.json"):
    path = config.project_root / name
    path.write_text(json.dumps({"changes": rules}), encoding="utf-8")
    return path


def _rule(key, field, direction="any", table="orders"):
    return {"table": table, "key": key, "field": field, "direction": direction}


def _partition(report, dt):
    return next(
        item for item in report["partition_checksum_mismatches"]
        if item["partition"] == {"dt": dt}
    )


def test_fully_declared_change_is_consistent(make_config, write_table):
    config = make_config()
    before, after = _write_pair(config, write_table, _change(BASE_ROWS, 2, amount=25.0))
    declaration = _declaration(config, [_rule(2, "amount", "increase")])
    report = run_audit(
        config, "orders", before, after, expected_changes=str(declaration)
    )
    assert report["status"] == "consistent"
    assert all(check["status"] == "pass" for check in report["checks"])
    assert report["counts"]["modified_rows"] == 1
    assert report["counts"]["expected_fields"] == 1
    assert report["counts"]["residual_modified_rows"] == 0
    assert report["residual"]["modified_rows"] == []
    assert report["expected_changes"]["modified_rows"] == [{
        "key": 2,
        "fields": [{"name": "amount", "before": 20.0, "after": 25.0}],
    }]
    assert report["expected_changes"]["counts"] == {
        "modified_rows": 1, "fields": 1, "partition_fields": 1, "partition_count": 1,
    }


def test_undeclared_field_in_exempted_partition_stays_residual(make_config, write_table):
    config = make_config()
    after_rows = _change(_change(BASE_ROWS, 1, amount=12.0), 2, status="refunded")
    before, after = _write_pair(config, write_table, after_rows)
    declaration = _declaration(config, [_rule(1, "amount", "increase")])
    report = run_audit(
        config, "orders", before, after, expected_changes=str(declaration)
    )
    assert report["status"] == "residual_diff"
    partition = _partition(report, "2024-02-01")
    assert partition["expected_fields"] == ["amount"]
    assert partition["residual_fields"] == ["status"]
    assert partition["mismatched_fields"] == ["amount", "status"]
    assert report["residual"]["modified_rows"] == [{
        "key": 2,
        "fields": [{"name": "status", "before": "paid", "after": "refunded"}],
    }]
    assert report["expected_changes"]["modified_rows"][0]["key"] == 1
    assert report["counts"]["expected_fields"] == 1
    assert report["counts"]["residual_modified_rows"] == 1
    check = next(
        item for item in report["checks"] if item["name"] == "partition_checksums"
    )
    assert check["status"] == "fail"


def test_partition_field_needs_every_change_declared(make_config, write_table):
    config = make_config()
    after_rows = _change(_change(BASE_ROWS, 1, amount=12.0), 2, amount=25.0)
    before, after = _write_pair(config, write_table, after_rows)
    declaration = _declaration(config, [_rule(1, "amount", "increase")])
    report = run_audit(
        config, "orders", before, after, expected_changes=str(declaration)
    )
    assert report["status"] == "residual_diff"
    partition = _partition(report, "2024-02-01")
    assert partition["mismatched_fields"] == ["amount"]
    assert partition["expected_fields"] == []
    assert partition["residual_fields"] == ["amount"]
    assert [item["key"] for item in report["residual"]["modified_rows"]] == [2]


def test_declared_direction_is_enforced(make_config, write_table):
    config = make_config()
    before, after = _write_pair(config, write_table, _change(BASE_ROWS, 2, amount=15.0))
    declaration = _declaration(config, [_rule(2, "amount", "increase")])
    report = run_audit(
        config, "orders", before, after, expected_changes=str(declaration)
    )
    assert report["status"] == "residual_diff"
    assert report["expected_changes"]["counts"]["fields"] == 0
    assert report["residual"]["modified_rows"][0]["key"] == 2
    assert _partition(report, "2024-02-01")["residual_fields"] == ["amount"]


def test_report_id_tracks_declaration_content(make_config, write_table):
    config = make_config()
    before, after = _write_pair(config, write_table, _change(BASE_ROWS, 2, amount=25.0))
    first = _declaration(config, [_rule(2, "amount", "increase")], "first.json")
    second = _declaration(config, [_rule(2, "amount", "any")], "second.json")
    no_decl = run_audit(config, "orders", before, after)
    with_first = run_audit(config, "orders", before, after, expected_changes=str(first))
    repeated = run_audit(config, "orders", before, after, expected_changes=str(first))
    with_second = run_audit(config, "orders", before, after, expected_changes=str(second))
    assert with_first["report_id"] == repeated["report_id"]
    assert with_first["report_id"] != no_decl["report_id"]
    assert with_first["report_id"] != with_second["report_id"]
    assert with_first["expected_changes"]["declaration"]["digest"] != (
        with_second["expected_changes"]["declaration"]["digest"]
    )
    loaded = load_expected_changes(str(first))
    inline = run_audit(
        config, "orders", before, after, expected_changes=[_rule(2, "amount", "increase")]
    )
    assert inline["report_id"] == with_first["report_id"]
    assert loaded.fingerprint_payload() == load_expected_changes(
        [_rule(2, "amount", "increase")]
    ).fingerprint_payload()


def test_undeclared_row_membership_is_never_excused(make_config, write_table):
    config = make_config()
    after_rows = BASE_ROWS + [
        {"order_id": 9, "amount": 99.0, "status": "new", "dt": "2024-02-02"}
    ]
    before, after = _write_pair(config, write_table, after_rows)
    declaration = _declaration(config, [_rule(9, "amount", "any")])
    report = run_audit(
        config, "orders", before, after, expected_changes=str(declaration)
    )
    assert report["status"] == "residual_diff"
    assert report["residual"]["extra_in_after"] == [9]
    assert report["counts"]["extra_in_after"] == 1


def test_declaration_formats_normalise_identically(make_config):
    config = make_config()
    path = config.project_root / "decl.toml"
    path.write_text(
        """
[[changes]]
table = "orders"
key = 2
fields = { amount = "increase", status = "any" }
""",
        encoding="utf-8",
    )
    from_file = load_expected_changes(str(path))
    from_mapping = load_expected_changes({
        "tables": {
            "orders": {
                "key": 2,
                "fields": {"amount": "up", "status": "changed"},
            }
        }
    })
    from_list = load_expected_changes([
        {"table": "orders", "key": 2, "field": "amount", "direction": "increase"},
        {"table": "orders", "key": 2, "field": "status"},
    ])
    expected = [
        ("orders", 2, "amount", "increase"),
        ("orders", 2, "status", "any"),
    ]
    for declaration in (from_file, from_mapping, from_list):
        assert [
            (rule["table"], rule["key"], rule["field"], rule["direction"])
            for rule in declaration.rules
        ] == expected
        assert declaration.digest == from_file.digest


@pytest.mark.parametrize(
    "document, message",
    [
        ({"changes": [{"table": "orders", "key": 1, "field": "amount",
                       "direction": "sideways"}]}, "direction"),
        ({"changes": [{"table": "orders", "key": 1}]}, "no fields"),
        ({"changes": [{"table": "orders", "field": "amount"}]}, "scalar key"),
        ({"changes": [{"table": "../orders", "key": 1, "field": "x"}]}, "table"),
        ({"changes": [{"table": "orders", "key": 1, "field": "amount",
                       "direction": "increase"},
                      {"table": "orders", "key": 1, "field": "amount",
                       "direction": "decrease"}]}, "conflicting"),
    ],
)
def test_invalid_declarations_are_rejected(document, message):
    with pytest.raises(ConfigError, match=message):
        load_expected_changes(document)


def test_declaration_unknown_field_or_bad_direction_type(make_config, write_table):
    config = make_config()
    before, after = _write_pair(config, write_table, BASE_ROWS)
    with pytest.raises(ConfigError, match="not in table"):
        run_audit(
            config, "orders", before, after,
            expected_changes=[_rule(1, "missing_field")],
        )
    with pytest.raises(ConfigError, match="numeric field"):
        run_audit(
            config, "orders", before, after,
            expected_changes=[_rule(1, "status", "increase")],
        )
    with pytest.raises(ConfigError, match="not found"):
        run_audit(
            config, "orders", before, after,
            expected_changes=str(config.project_root / "absent.toml"),
        )


def test_declaration_key_type_must_match_primary_key(make_config, write_table):
    config = make_config()
    before, after = _write_pair(config, write_table, BASE_ROWS)
    with pytest.raises(ConfigError, match="is a str value"):
        run_audit(
            config, "orders", before, after,
            expected_changes=[_rule("1", "amount")],
        )


def test_numeric_keys_match_across_int_float_spelling(make_config, write_table):
    config = make_config()
    before, after = _write_pair(config, write_table, _change(BASE_ROWS, 2, amount=25.0))
    report = run_audit(
        config, "orders", before, after,
        expected_changes=[_rule(2.0, "amount", "increase")],
    )
    assert report["status"] == "consistent"
    assert report["expected_changes"]["counts"]["fields"] == 1
    spelled_int = run_audit(
        config, "orders", before, after,
        expected_changes=[_rule(2, "amount", "increase")],
    )
    assert spelled_int["report_id"] == report["report_id"]



def test_structural_mismatch_still_wins_over_declaration(make_config, write_table):
    config = make_config()
    after_rows = _change(BASE_ROWS, 3, dt="2024-02-03")
    before, after = _write_pair(config, write_table, after_rows)
    declaration = _declaration(config, [
        _rule(1, "amount"), _rule(2, "amount"), _rule(3, "dt"),
    ])
    report = run_audit(
        config, "orders", before, after, expected_changes=str(declaration)
    )
    assert report["status"] == "structural_mismatch"
    check_map = {check["name"]: check["status"] for check in report["checks"]}
    assert check_map["partition_structure"] == "fail"


def test_declaration_for_other_table_does_not_excuse_changes(make_config, write_table):
    config = make_config()
    before, after = _write_pair(config, write_table, _change(BASE_ROWS, 2, amount=25.0))
    declaration = _declaration(config, [_rule(2, "amount", "increase", table="users")])
    report = run_audit(
        config, "orders", before, after, expected_changes=str(declaration)
    )
    assert report["status"] == "residual_diff"
    assert report["expected_changes"]["declaration"]["table_rules"] == 0
    assert report["residual"]["modified_rows"][0]["key"] == 2


def test_cli_exposes_expectations_flag(make_config, write_table, capsys):
    config = make_config()
    before, after = _write_pair(config, write_table, _change(BASE_ROWS, 2, amount=25.0))
    declaration = _declaration(config, [_rule(2, "amount", "increase")])
    code = main([
        "--config", str(config.path), "backfill-audit", "--table", "orders",
        "--before", str(before), "--after", str(after),
        "--expectations", str(declaration), "--json",
    ])
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["status"] == "consistent"
    assert payload["expected_changes"]["counts"]["fields"] == 1
    alias_code = main([
        "--config", str(config.path), "backfill-audit", "--table", "orders",
        "--before", str(before), "--after", str(after),
        "--expected-changes", str(declaration),
    ])
    assert alias_code == 0
    capsys.readouterr()
    missing_code = main([
        "--config", str(config.path), "backfill-audit", "--table", "orders",
        "--before", str(before), "--after", str(after),
        "--expectations", str(config.project_root / "absent.toml"), "--json",
    ])
    captured = capsys.readouterr()
    assert missing_code == 2
    assert json.loads(captured.err)["error"]["code"] == "config_error"


def test_cli_without_declaration_is_unchanged(make_config, write_table, capsys):
    config = make_config()
    before, after = _write_pair(config, write_table, _change(BASE_ROWS, 2, amount=25.0))
    code = main([
        "--config", str(config.path), "backfill-audit", "--table", "orders",
        "--before", str(before), "--after", str(after), "--json",
    ])
    payload = json.loads(capsys.readouterr().out)
    assert code == 4
    assert payload["status"] == "residual_diff"
    assert "expected_changes" not in payload
    assert payload["residual"]["modified_rows"][0]["key"] == 2


def test_markdown_and_json_report_show_expected_split(make_config, write_table):
    config = make_config()
    after_rows = _change(_change(BASE_ROWS, 1, amount=12.0), 2, status="refunded")
    before, after = _write_pair(config, write_table, after_rows)
    declaration = _declaration(config, [_rule(1, "amount", "increase")])
    report = run_audit(
        config, "orders", before, after, expected_changes=str(declaration)
    )
    markdown = (
        config.reports_dir / f"{report['report_id']}.md"
    ).read_text(encoding="utf-8")
    assert "## Expected Changes" in markdown
    assert "Expected key `1` fields: amount" in markdown
    assert "expected: amount" in markdown
    assert "residual: status" in markdown
    assert f"--expectations {declaration}" in markdown
    stored = json.loads(
        (config.reports_dir / f"{report['report_id']}.json").read_text()
    )
    partition = next(
        item for item in stored["partition_checksum_mismatches"]
        if item["partition"] == {"dt": "2024-02-01"}
    )
    assert partition["expected_fields"] == ["amount"]
    assert partition["residual_fields"] == ["status"]
    assert stored["counts"]["expected_fields"] == 1
    assert stored["counts"]["residual_modified_rows"] == 1

