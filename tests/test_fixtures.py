from __future__ import annotations

import pytest

from lakeback.backends import get_backend
from lakeback.errors import ConfigError
from lakeback.fixtures.generate import generate_fixtures
from lakeback._util import tree_digest


def test_fixture_generation_is_byte_deterministic(make_config):
    config = make_config()
    first = generate_fixtures(config)
    second = generate_fixtures(config)
    assert first["digests"] == second["digests"]
    assert tree_digest(config.fixtures_dir / "before") == first["digests"]["before"]
    assert first["digests"]["lake"] == first["digests"]["after"]
    assert first["tables"]["orders"]["before_rows"] == 12
    assert first["tables"]["orders"]["after_rows"] == 12
    assert first["tables"]["users"]["before_rows"] == 6
    assert set(first["tables"]) == {"orders", "users", "refunds"}


def test_generated_tables_are_partitioned_and_typed(make_config):
    config = make_config()
    generate_fixtures(config)
    backend = get_backend("csv")
    before = backend.read_table(config.fixtures_dir / "before" / "orders")
    after = backend.read_table(config.fixtures_dir / "after" / "orders")
    assert len(before) == len(after) == 12
    before_ids = {row["order_id"] for row in before}
    after_ids = {row["order_id"] for row in after}
    assert sorted(before_ids) == list(range(100, 112))
    assert len(before_ids - after_ids) == 1
    assert after_ids - before_ids == {112}
    assert len(after_ids) == 12
    files = backend.scan(config.fixtures_dir / "before" / "orders")
    assert len(files) == 3
    assert {item.partition["dt"] for item in files} == {
        "2024-02-01", "2024-02-02", "2024-02-03"
    }
    assert all(isinstance(row["amount"], float) for row in before)
    assert all(row["status"] in {"paid", "pending", "refunded"} for row in before)


def test_before_and_after_differ_by_documented_backfill(make_config):
    config = make_config()
    generate_fixtures(config)
    backend = get_backend("csv")
    before = {row["order_id"]: row for row in backend.read_table(
        config.fixtures_dir / "before" / "orders"
    )}
    after = {row["order_id"]: row for row in backend.read_table(
        config.fixtures_dir / "after" / "orders"
    )}
    assert set(before) - set(after)
    assert set(after) - set(before)
    shared = set(before) & set(after)
    assert any(before[key] != after[key] for key in shared)


def test_missing_seed_is_reported(make_config):
    config = make_config()
    with pytest.raises(ConfigError, match="seed file not found"):
        generate_fixtures(config, seed_path=config.project_root / "absent.toml")


def test_invalid_seed_is_reported(make_config):
    config = make_config()
    seed = config.project_root / "bad.toml"
    seed.write_text("seed = 1\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="partitioning"):
        generate_fixtures(config, seed_path=seed)


def test_seed_override_changes_row_count(make_config):
    config = make_config()
    original = (config.project_root / "seed.toml").read_text(encoding="utf-8")
    changed = original.replace("rows = 12", "rows = 9", 1)
    path = config.project_root / "seed_override.toml"
    path.write_text(changed, encoding="utf-8")
    result = generate_fixtures(config, seed_path=path)
    assert result["tables"]["orders"]["before_rows"] == 9
