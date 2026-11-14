from __future__ import annotations

import json

import pytest

from lakeback.cli import main
from lakeback.errors import BackendError, DataError, LedgerError, NotFoundError, RequestError
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.snapshotter import create_snapshot
from lakeback.travel.referential import audit_referential_integrity

FACT_SCHEMA = {"order_id": "int", "user_id": "int", "region": "str", "dt": "str"}
DIM_SCHEMA = {"user_id": "int", "region": "str", "tier": "str", "dt": "str"}


def _group(rows, column="dt"):
    grouped = {}
    for row in rows:
        grouped.setdefault(row[column], []).append(row)
    return grouped


def _snapshot(config, write_table, table, rows, schema):
    write_table(config.lake_root / table, _group(rows), schema)
    return create_snapshot(config, table)


def _fact(order_id, user_id, region="north", dt="2024-02-01"):
    return {"order_id": order_id, "user_id": user_id, "region": region, "dt": dt}


def _dim(user_id, region="north", tier="basic", dt="2024-02-01"):
    return {"user_id": user_id, "region": region, "tier": tier, "dt": dt}


def _base(config, write_table, fact_rows, dim_rows):
    _snapshot(config, write_table, "users", dim_rows, DIM_SCHEMA)
    return _snapshot(config, write_table, "orders", fact_rows, FACT_SCHEMA)


def _ledger(config):
    return Ledger(config.ledger_dir)


def _audit(config, fact_version=1, dimension_version=1, foreign_key=None, **kwargs):
    return audit_referential_integrity(
        config,
        _ledger(config),
        "orders",
        fact_version,
        "users",
        dimension_version,
        foreign_key or {"user_id": "user_id"},
        **kwargs,
    )


def test_all_foreign_keys_resolve(make_config, write_table):
    config = make_config()
    _base(
        config, write_table,
        [_fact(1, 10), _fact(2, 11), _fact(3, 10)],
        [_dim(10), _dim(11), _dim(12)],
    )
    result = _audit(config)
    assert result["status"] == "consistent"
    assert result["counts"] == {
        "fact_rows": 3,
        "dimension_rows": 3,
        "checked": 3,
        "matched": 3,
        "ambiguous_parent_matches": 0,
        "orphans": 0,
        "null_skipped": 0,
        "dimension_null_key_rows": 0,
        "parent_key_duplicates": 0,
        "parent_key_duplicate_rows": 0,
    }
    assert result["orphans"] == []
    assert result["parent_key_duplicates"] == []
    assert all(check["status"] == "pass" for check in result["checks"])
    assert result["fact"]["version"] == result["dimension"]["version"] == 1


def test_orphans_are_reported_separately_from_duplicate_parent_keys(
    make_config, write_table
):
    config = make_config()
    _base(
        config, write_table,
        [_fact(1, 10), _fact(2, 99), _fact(3, 12), _fact(4, 98)],
        [_dim(10), _dim(11), _dim(12), _dim(12)],
    )
    result = _audit(config)
    assert result["status"] == "orphan_references_and_duplicate_parent_keys"
    assert result["counts"]["orphans"] == 2
    assert result["counts"]["parent_key_duplicates"] == 1
    assert result["counts"]["parent_key_duplicate_rows"] == 2
    assert result["counts"]["ambiguous_parent_matches"] == 1
    assert result["counts"]["matched"] == 1
    assert [item["foreign_key"] for item in result["orphans"]] == [
        {"user_id": 98}, {"user_id": 99},
    ]
    assert result["ambiguous_matches"] == [{
        "row_index": 2,
        "foreign_key": {"user_id": 12},
        "row": _fact(3, 12),
        "parent_row_indexes": [2, 3],
    }]
    duplicate = result["parent_key_duplicates"][0]
    assert duplicate["key"] == {"user_id": 12}
    assert duplicate["row_count"] == 2
    assert len(duplicate["rows"]) == 2
    checks = {check["name"]: check["status"] for check in result["checks"]}
    assert checks["referential_integrity"] == "fail"
    assert checks["parent_key_uniqueness"] == "fail"
    # Duplicate parents are never silently collapsed into a match or an orphan.
    assert result["counts"]["matched"] + result["counts"]["ambiguous_parent_matches"] + (
        result["counts"]["orphans"]
    ) == result["counts"]["fact_rows"]


def test_duplicate_parent_keys_alone_are_not_reported_as_orphans(
    make_config, write_table
):
    config = make_config()
    _base(
        config, write_table,
        [_fact(1, 10)],
        [_dim(10), _dim(10)],
    )
    result = _audit(config)
    assert result["status"] == "duplicate_parent_keys"
    assert result["counts"]["orphans"] == 0
    assert result["counts"]["ambiguous_parent_matches"] == 1
    assert result["counts"]["matched"] == 0


def test_composite_key_is_compared_as_typed_tuple(make_config, write_table):
    config = make_config()
    _base(
        config, write_table,
        [_fact(1, 10, "north"), _fact(2, 10, "south"), _fact(3, 11, "north")],
        [_dim(10, "north"), _dim(11, "south")],
    )
    result = _audit(config, foreign_key={"user_id": "user_id", "region": "region"})
    assert result["status"] == "orphan_references"
    assert result["counts"]["matched"] == 1
    assert [item["foreign_key"] for item in result["orphans"]] == [
        {"user_id": 10, "region": "south"},
        {"user_id": 11, "region": "north"},
    ]
    assert [pair["fact_column"] for pair in result["foreign_key"]] == [
        "user_id", "region"
    ]


def test_composite_key_does_not_collide_across_field_boundaries(
    make_config, write_table
):
    """Values that would collide when concatenated must not match."""
    config = make_config()
    fact_schema = {"order_id": "int", "a": "str", "b": "str", "dt": "str"}
    dim_schema = {"pk": "int", "a": "str", "b": "str", "dt": "str"}
    _snapshot(
        config, write_table, "users",
        [{"pk": 1, "a": "x", "b": "yz", "dt": "2024-02-01"}], dim_schema,
    )
    _snapshot(
        config, write_table, "orders",
        [{"order_id": 1, "a": "xy", "b": "z", "dt": "2024-02-01"}], fact_schema,
    )
    result = _audit(config, foreign_key={"a": "a", "b": "b"})
    assert result["counts"]["matched"] == 0
    assert result["counts"]["orphans"] == 1
    assert result["orphans"][0]["foreign_key"] == {"a": "xy", "b": "z"}


def test_int_and_float_keys_normalise_to_a_comparable_type(make_config, write_table):
    config = make_config()
    fact_schema = {"order_id": "int", "user_id": "float", "dt": "str"}
    _snapshot(config, write_table, "users", [_dim(10)], DIM_SCHEMA)
    _snapshot(
        config, write_table, "orders",
        [{"order_id": 1, "user_id": 10.0, "dt": "2024-02-01"}], fact_schema,
    )
    result = _audit(config)
    assert result["foreign_key"][0]["compare_type"] == "numeric"
    assert result["counts"]["matched"] == 1


def test_large_integer_key_is_not_rounded_to_float(make_config, write_table):
    """A float cast would round 2**53 + 1 onto 2**53 and fake a match."""
    config = make_config()
    big = 2 ** 53 + 1
    fact_schema = {"order_id": "int", "user_id": "int", "dt": "str"}
    dim_schema = {"user_id": "float", "region": "str", "dt": "str"}
    _snapshot(
        config, write_table, "users",
        [{"user_id": float(2 ** 53), "region": "north", "dt": "2024-02-01"}],
        dim_schema,
    )
    _snapshot(
        config, write_table, "orders",
        [{"order_id": 1, "user_id": big, "dt": "2024-02-01"}], fact_schema,
    )
    result = _audit(config)
    assert result["counts"]["matched"] == 0
    assert result["counts"]["orphans"] == 1
    assert result["orphans"][0]["foreign_key"] == {"user_id": big}


def test_missing_foreign_key_column_is_rejected(make_config, write_table):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    with pytest.raises(DataError, match="missing from the fact snapshot schema") as err:
        _audit(config, foreign_key={"nope": "user_id"})
    assert err.value.details["missing_from"] == "fact"
    with pytest.raises(DataError, match="missing from the dimension snapshot schema"):
        _audit(config, foreign_key={"user_id": "nope"})


def test_incompatible_key_types_are_rejected_not_counted_as_orphans(
    make_config, write_table
):
    config = make_config()
    fact_schema = {"order_id": "int", "user_id": "str", "dt": "str"}
    _snapshot(config, write_table, "users", [_dim(10)], DIM_SCHEMA)
    _snapshot(
        config, write_table, "orders",
        [{"order_id": 1, "user_id": "abc", "dt": "2024-02-01"}], fact_schema,
    )
    with pytest.raises(DataError, match="incompatible foreign-key types") as err:
        _audit(config)
    assert err.value.details["fact_type"] == "str"
    assert err.value.details["dimension_type"] == "int"


def test_null_foreign_key_is_skipped_by_default(make_config, write_table):
    config = make_config()
    _base(
        config, write_table,
        [_fact(1, 10), {"order_id": 2, "user_id": None, "region": "north",
                        "dt": "2024-02-01"}],
        [_dim(10)],
    )
    result = _audit(config)
    assert result["status"] == "consistent"
    assert result["counts"]["null_skipped"] == 1
    assert result["counts"]["matched"] == 1
    assert result["counts"]["orphans"] == 0


def test_null_policy_error_rejects_null_components(make_config, write_table):
    config = make_config()
    _base(
        config, write_table,
        [_fact(1, 10), {"order_id": 2, "user_id": None, "region": "north",
                        "dt": "2024-02-01"}],
        [_dim(10)],
    )
    with pytest.raises(DataError, match="null_policy is 'error'"):
        _audit(config, null_policy="error")


def test_partial_null_composite_component_skips_whole_row(make_config, write_table):
    config = make_config()
    _base(
        config, write_table,
        [_fact(1, 10, "north"), _fact(2, 10, None)],
        [_dim(10, "north")],
    )
    result = _audit(
        config, foreign_key={"user_id": "user_id", "region": "region"},
        null_policy="skip",
    )
    assert result["counts"]["null_skipped"] == 1
    assert result["counts"]["matched"] == 1
    assert result["counts"]["orphans"] == 0


def test_dimension_rows_with_null_key_never_match(make_config, write_table):
    """NULL components on the parent side are not candidates (NULL != NULL)."""
    config = make_config()
    _base(
        config, write_table,
        [_fact(1, 10, "north")],
        [_dim(10, None)],
    )
    result = _audit(
        config, foreign_key={"user_id": "user_id", "region": "region"},
        null_policy="skip",
    )
    assert result["counts"]["dimension_null_key_rows"] == 1
    assert result["counts"]["matched"] == 0
    assert result["counts"]["orphans"] == 1


def test_int_float_values_share_duplicate_key_detection(make_config, write_table):
    config = make_config()
    fact_schema = {"order_id": "int", "user_id": "int", "dt": "str"}
    dim_schema = {"user_id": "float", "region": "str", "dt": "str"}
    _snapshot(
        config, write_table, "users",
        [
            {"user_id": 10.0, "region": "north", "dt": "2024-02-01"},
            {"user_id": 10.0, "region": "north", "dt": "2024-02-01"},
        ],
        dim_schema,
    )
    _snapshot(
        config, write_table, "orders",
        [{"order_id": 1, "user_id": 10, "dt": "2024-02-01"}], fact_schema,
    )
    result = _audit(config)
    assert result["counts"]["parent_key_duplicates"] == 1
    assert result["counts"]["ambiguous_parent_matches"] == 1


def test_only_pinned_versions_are_read_not_latest(make_config, write_table):
    config = make_config()
    _base(config, write_table, [_fact(1, 10), _fact(2, 99)], [_dim(10)])
    # Version 2 introduces the missing parent; auditing v1 must still see the orphan.
    _snapshot(config, write_table, "users", [_dim(10), _dim(99)], DIM_SCHEMA)
    result = _audit(config, dimension_version=1)
    assert result["dimension"]["version"] == 1
    assert result["counts"]["orphans"] == 1
    fresh = _audit(config, dimension_version=2)
    assert fresh["dimension"]["version"] == 2
    assert fresh["counts"]["orphans"] == 0


@pytest.mark.parametrize("selector", ["latest", "2025-01-01T00:00:00Z", "", None, "v0"])
def test_explicit_versions_are_required(make_config, write_table, selector):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    with pytest.raises(RequestError):
        _audit(config, fact_version=selector)
    with pytest.raises(RequestError):
        _audit(config, dimension_version=selector)


@pytest.mark.parametrize("selector", [1, "1", "v1"])
def test_integer_and_v_prefixed_versions_are_accepted(
    make_config, write_table, selector
):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    result = _audit(config, fact_version=selector, dimension_version=selector)
    assert result["fact"]["version"] == 1
    assert result["dimension"]["version"] == 1


def test_missing_version_is_not_found(make_config, write_table):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    with pytest.raises(NotFoundError, match="no snapshot version 9"):
        _audit(config, dimension_version=9)


def test_snapshot_checksum_is_verified_after_reading(make_config, write_table):
    config = make_config()
    manifest = _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    stored = config.snapshot_store / manifest["snapshot_id"] / manifest["files"][0]["path"]
    stored.write_bytes(stored.read_bytes() + b"corruption")
    with pytest.raises(BackendError, match="checksum mismatch"):
        _audit(config)


def test_rewritten_history_is_rejected(make_config, write_table):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    ledger_path = config.ledger_dir / "snapshots.jsonl"
    line = ledger_path.read_text(encoding="utf-8").splitlines()[0]
    record = json.loads(line)
    record["manifest"]["created_at"] = "2030-01-01T00:00:00Z"
    ledger_path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    with pytest.raises(LedgerError, match="modified"):
        _audit(config)


def test_ledger_written_during_read_is_detected(make_config, write_table, monkeypatch):
    """A history change after both reads but before the post-read check fails."""
    import lakeback.travel.referential as referential

    config = make_config()
    manifest = _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    original = referential.verify_snapshot_files

    def mutate(config_arg, manifest_arg, **kwargs):
        ledger_path = config.ledger_dir / "snapshots.jsonl"
        lines = ledger_path.read_text(encoding="utf-8").splitlines()
        record = json.loads(lines[-1])
        record["manifest"]["created_at"] = "2031-01-01T00:00:00Z"
        lines[-1] = json.dumps(record)
        ledger_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return original(config_arg, manifest_arg, **kwargs)

    monkeypatch.setattr(referential, "verify_snapshot_files", mutate)
    with pytest.raises(LedgerError):
        _audit(config)
    assert manifest["version"] == 1


def test_audit_is_deterministic(make_config, write_table):
    config = make_config()
    _base(
        config, write_table,
        [_fact(1, 10), _fact(2, 98), _fact(3, 99)],
        [_dim(10), _dim(11), _dim(11)],
    )
    first = _audit(config)
    second = _audit(config)
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_audit_reads_fact_and_dimension_from_own_snapshots(make_config, write_table):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    # Overwriting the current lake table must not change the pinned audit.
    write_table(config.lake_root / "orders", _group([_fact(1, 999)]), FACT_SCHEMA)
    result = _audit(config)
    assert result["counts"]["orphans"] == 0
    assert result["counts"]["matched"] == 1


def test_null_policy_validation(make_config, write_table):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    with pytest.raises(RequestError, match="null_policy"):
        _audit(config, null_policy="ignore")


def test_verified_block_records_checks_and_history(make_config, write_table):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    result = _audit(config)
    verified = result["verified"]
    assert verified["snapshot_files_sha256"] == {"fact": 1, "dimension": 1}
    assert verified["history_unchanged"] is True
    assert verified["ledger_record_count"] == 2
    assert set(verified["history_digests"]) == {"orders@1", "users@1"}


def test_cli_referential_audit_json_and_exit_codes(make_config, write_table, capsys):
    config = make_config()
    _base(config, write_table, [_fact(1, 10), _fact(2, 99)], [_dim(10)])
    code = main([
        "--config", str(config.path), "referential-audit",
        "--fact-table", "orders", "--fact-version", "1",
        "--dimension-table", "users", "--dimension-version", "1",
        "--key", "user_id=user_id", "--json",
    ])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 4
    assert payload["status"] == "orphan_references"
    assert payload["counts"]["orphans"] == 1
    assert "orphans=1" in captured.err


def test_cli_referential_audit_consistent_exit_zero(make_config, write_table, capsys):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    code = main([
        "--config", str(config.path), "referential-audit",
        "--fact-table", "orders", "--fact-version", "1",
        "--dimension-table", "users", "--dimension-version", "1",
        "--key", "user_id=user_id",
    ])
    captured = capsys.readouterr()
    assert code == 0
    assert "status=consistent" in captured.out
    assert captured.err == ""


@pytest.mark.parametrize(
    "extra, expected",
    [
        (["--key", "broken"], 2),
        (["--key", "user_id=user_id", "--fact-version", "latest"], 2),
        (["--key", "user_id=user_id", "--dimension-version", "9"], 3),
        (["--key", "nope=user_id"], 2),
    ],
)
def test_cli_referential_audit_error_exit_codes(
    make_config, write_table, capsys, extra, expected
):
    config = make_config()
    _base(config, write_table, [_fact(1, 10)], [_dim(10)])
    arguments = [
        "--config", str(config.path), "referential-audit",
        "--fact-table", "orders", "--dimension-table", "users",
    ]
    arguments.extend(extra)
    if "--fact-version" not in extra:
        arguments.extend(["--fact-version", "1"])
    if "--dimension-version" not in extra:
        arguments.extend(["--dimension-version", "1"])
    code = main(arguments)
    captured = capsys.readouterr()
    assert code == expected
    assert captured.err.startswith("lakeback:")


def test_old_diff_behaviour_is_unchanged(make_config, write_table, capsys):
    config = make_config()
    _snapshot(config, write_table, "orders", [_fact(1, 10)], FACT_SCHEMA)
    _snapshot(config, write_table, "orders", [_fact(1, 10), _fact(2, 11)], FACT_SCHEMA)
    code = main([
        "--config", str(config.path), "diff", "--table", "orders",
        "--from", "1", "--to", "2", "--json",
    ])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["counts"]["added"] == 1
    assert payload["counts"]["removed"] == 0
