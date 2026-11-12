from __future__ import annotations

import ipaddress
from pathlib import Path

import pytest

from lakeback.backends import get_backend
from lakeback.backends import parquet_backend
from lakeback.config import Config, validate_host
from lakeback.errors import ConfigError

NON_LOOPBACK = str(ipaddress.ip_address(1))


def test_load_resolves_paths_and_fixed_clock(make_config):
    config = make_config()
    assert config.backend == "csv"
    assert config.lake_root == config.project_root / "lake"
    assert config.ledger_dir == config.project_root / "ledger"
    assert config.seed_path.is_file()
    assert config.host == "127.0.0.1"
    assert config.now_iso() == "2025-01-01T00:00:00Z"
    assert config.table_key("orders") == "order_id"
    assert config.table_keys["users"] == "user_id"


def test_missing_config_is_actionable(tmp_path):
    with pytest.raises(ConfigError, match="configuration file not found"):
        Config.load(tmp_path / "missing.toml")


def test_invalid_toml_is_rejected(tmp_path):
    path = tmp_path / "bad.toml"
    path.write_text("[paths\nlake = 1\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="cannot read configuration"):
        Config.load(path)


def test_invalid_backend_is_rejected(make_config, tmp_path):
    config = make_config()
    text = config.path.read_text(encoding="utf-8").replace('backend = "csv"', 'backend = "duck"')
    config.path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError, match="unsupported backend"):
        Config.load(config.path)


def test_non_loopback_host_is_rejected(make_config):
    config = make_config()
    text = config.path.read_text(encoding="utf-8").replace(
        'host = "127.0.0.1"', f'host = "{NON_LOOPBACK}"'
    )
    config.path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError, match="non-loopback"):
        Config.load(config.path)


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "::1"])
def test_loopback_hosts_are_allowed(host):
    assert validate_host(host) == host


@pytest.mark.parametrize("host", [NON_LOOPBACK, "example.invalid", ""])
def test_bad_host_values_fail(host):
    with pytest.raises(ConfigError):
        validate_host(host)


def test_fixed_clock_requires_timestamp(make_config):
    config = make_config()
    text = config.path.read_text(encoding="utf-8").replace(
        'fixed_time = "2025-01-01T00:00:00Z"\n', ""
    )
    config.path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError, match="fixed_time"):
        Config.load(config.path)


def test_invalid_clock_mode(make_config):
    config = make_config()
    text = config.path.read_text(encoding="utf-8").replace('mode = "fixed"', 'mode = "warp"')
    config.path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError, match=r"\[clock\]\.mode"):
        Config.load(config.path)


def test_unavailable_parquet_backend_explains_fallback(make_config, monkeypatch):
    config = make_config()
    monkeypatch.setattr(parquet_backend, "PARQUET_AVAILABLE", False)
    with pytest.raises(ConfigError) as caught:
        config.resolve_backend("parquet")
    assert "pyarrow" in caught.value.message
    assert "csv" in caught.value.details["hint"]


def test_unknown_backend_name():
    with pytest.raises(ValueError, match="unknown backend"):
        get_backend("nope")


def test_missing_table_key_is_explicit(make_config):
    config = make_config()
    with pytest.raises(ConfigError, match="no primary key"):
        config.table_key("unknown_table")


def test_ensure_runtime_dirs(make_config):
    config = make_config()
    config.ensure_runtime_dirs()
    for path in (
        config.lake_root,
        config.ledger_dir,
        config.snapshot_store,
        config.reports_dir,
    ):
        assert Path(path).is_dir()


def test_describe_contains_no_secrets(make_config):
    description = make_config().describe()
    assert description["service"]["host"] == "127.0.0.1"
    assert "password" not in str(description).lower()
    assert "token" not in str(description).lower()
