from __future__ import annotations

import json
import shutil

import pytest

from lakeback.backends import parquet_backend
from lakeback.cli import main


def _run(capsys, config, *arguments):
    code = main(["--config", str(config.path), *arguments])
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _json_run(capsys, config, *arguments):
    code, out, err = _run(capsys, config, *arguments, "--json")
    payload = json.loads(out) if out.strip() else None
    return code, payload, err


def _prepare_versions(capsys, config):
    code, _, _ = _json_run(capsys, config, "snapshot", "--table", "orders")
    assert code == 0
    shutil.rmtree(config.lake_root / "orders")
    shutil.copytree(config.fixtures_dir / "before" / "orders", config.lake_root / "orders")
    code, _, _ = _json_run(capsys, config, "snapshot", "--table", "orders")
    assert code == 0


def test_doctor_and_selftest_exit_zero(make_config, capsys):
    config = make_config()
    code, payload, err = _json_run(capsys, config, "doctor")
    assert code == 0
    assert payload["ok"] is True
    assert payload["configured_backend"] == "csv"
    assert err == ""
    code, payload, err = _json_run(capsys, config, "selftest", "--backend", "csv")
    assert code == 0
    assert payload["ok"] is True
    assert all(value == "ok" for value in payload["checks"].values())


def test_doctor_reports_unavailable_backend(make_config, capsys, monkeypatch):
    config = make_config(backend="parquet")
    monkeypatch.setattr(parquet_backend, "PARQUET_AVAILABLE", False)
    code, payload, _ = _json_run(capsys, config, "doctor")
    assert code == 2
    assert payload["ok"] is False
    assert payload["configured_backend_available"] is False
    assert "pyarrow" in payload["hint"]


def test_gen_fixtures_is_deterministic_through_cli(make_config, capsys):
    config = make_config()
    code, first, _ = _json_run(capsys, config, "gen-fixtures", "--backend", "csv")
    assert code == 0
    code, second, _ = _json_run(capsys, config, "gen-fixtures", "--backend", "csv")
    assert code == 0
    assert first["digests"] == second["digests"]
    assert first["paths"]["lake"] == second["paths"]["lake"]
    assert first["tables"]["orders"]["before_rows"] == 12


def test_snapshot_travel_diff_and_audit_flow(make_config, capsys):
    config = make_config()
    code, generated, _ = _json_run(capsys, config, "gen-fixtures")
    assert code == 0
    _prepare_versions(capsys, config)
    code, travel, _ = _json_run(
        capsys, config, "travel", "--table", "orders", "--at", "2", "--limit", "2"
    )
    assert code == 0
    assert travel["snapshot"]["version"] == 2
    assert travel["returned"] == 2
    code, diff, _ = _json_run(
        capsys, config, "diff", "--table", "orders", "--from", "2", "--to", "1"
    )
    assert code == 0
    assert generated["tables"]["orders"]["after_rows"] == 12
    assert diff["counts"]["added"] == 1
    assert diff["counts"]["removed"] == 1
    assert diff["counts"]["modified"] == 1
    code, report, stderr = _json_run(
        capsys, config, "backfill-audit", "--table", "orders",
        "--before", str(config.fixtures_dir / "before" / "orders"),
        "--after", str(config.fixtures_dir / "after" / "orders"),
    )
    assert code == 4
    assert report["status"] == "residual_diff"
    assert "audit status=residual_diff" in stderr
    assert report["artifacts"]["json_report"]


def test_audit_consistent_exit_zero(make_config, capsys):
    config = make_config()
    code, _, _ = _json_run(capsys, config, "gen-fixtures")
    assert code == 0
    target = config.fixtures_dir / "after" / "orders"
    code, report, _ = _json_run(
        capsys, config, "backfill-audit", "--table", "orders",
        "--before", str(target), "--after", str(target),
    )
    assert code == 0
    assert report["status"] == "consistent"


def test_missing_version_exit_three(make_config, capsys):
    config = make_config()
    code, generated, _ = _json_run(capsys, config, "gen-fixtures")
    assert code == 0
    code, payload, _ = _json_run(capsys, config, "snapshot", "--table", "orders")
    assert code == 0
    code, out, err = _run(capsys, config, "travel", "--table", "orders", "--at", "99")
    assert code == 3
    assert out == ""
    assert "no snapshot version 99" in err


@pytest.mark.parametrize(
    "arguments",
    [
        ["travel", "--table", "orders", "--at", "1", "--partition", "broken"],
        ["travel", "--table", "orders", "--at", "1", "--limit", "-2"],
        ["snapshot", "--table", "../escape"],
    ],
)
def test_usage_errors_exit_two(make_config, capsys, arguments):
    config = make_config()
    code, _, err = _run(capsys, config, *arguments)
    assert code == 2
    assert err.startswith("lakeback:")


def test_missing_config_exit_two(tmp_path, capsys):
    code = main(["--config", str(tmp_path / "absent.toml"), "doctor"])
    captured = capsys.readouterr()
    assert code == 2
    assert "configuration file not found" in captured.err


def test_cli_json_errors_are_structured(make_config, capsys):
    config = make_config()
    code, out, err = _run(capsys, config, "travel", "--table", "orders", "--at", "1", "--json")
    assert code == 3
    assert out == ""
    payload = json.loads(err)
    assert payload["error"]["code"] == "not_found"
