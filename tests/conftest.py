from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lakeback.backends import get_backend  # noqa: E402
from lakeback.config import Config  # noqa: E402
from lakeback.fixtures.generate import generate_fixtures  # noqa: E402

SMALL_SEED = """\
seed = 7

[partitioning]
column = "dt"
values = ["2024-02-01", "2024-02-02", "2024-02-03"]

[backfill]
table = "orders"
modify_rows = 1
delete_rows = 1
add_rows = 1

[tables.orders]
rows = 12
key = "order_id"
id_start = 100

[[tables.orders.columns]]
name = "order_id"
type = "int"

[[tables.orders.columns]]
name = "user_id"
type = "int"

[[tables.orders.columns]]
name = "amount"
type = "float"

[[tables.orders.columns]]
name = "status"
type = "str"

[[tables.orders.columns]]
name = "dt"
type = "str"

[tables.users]
rows = 6
key = "user_id"
id_start = 500

[[tables.users.columns]]
name = "user_id"
type = "int"

[[tables.users.columns]]
name = "region"
type = "str"

[[tables.users.columns]]
name = "dt"
type = "str"

[tables.refunds]
rows = 5
key = "refund_id"
id_start = 900

[[tables.refunds.columns]]
name = "refund_id"
type = "int"

[[tables.refunds.columns]]
name = "order_id"
type = "int"

[[tables.refunds.columns]]
name = "amount"
type = "float"

[[tables.refunds.columns]]
name = "dt"
type = "str"
"""


@pytest.fixture
def make_config(tmp_path):
    """Create an isolated project root with a valid lakeback.toml."""

    def _make(
        name: str = "project",
        backend: str = "csv",
        clock_mode: str = "fixed",
        fixed_time: str = "2025-01-01T00:00:00Z",
        host: str = "127.0.0.1",
    ) -> Config:
        root = tmp_path / name
        root.mkdir(parents=True, exist_ok=True)
        seed = root / "seed.toml"
        seed.write_text(SMALL_SEED, encoding="utf-8")
        clock = '[clock]\nmode = "system"\n'
        if clock_mode == "fixed":
            clock = f'[clock]\nmode = "fixed"\nfixed_time = "{fixed_time}"\n'
        config_text = f"""\
[paths]
lake = "lake"
ledger = "ledger"
snapshots = "snapshots"
reports = "reports"
fixtures = "fixtures"
seed = "seed.toml"

[storage]
backend = "{backend}"

[service]
host = "{host}"
port = 5000

{clock}
[keys]
orders = "order_id"
users = "user_id"
refunds = "refund_id"
"""
        path = root / "lakeback.toml"
        path.write_text(config_text, encoding="utf-8")
        return Config.load(path)

    return _make


@pytest.fixture
def seeded(make_config):
    config = make_config()
    generate_fixtures(config)
    return config


@pytest.fixture
def write_table():
    """Write a complete table directory with one file per partition value."""

    def _write(
        table_dir: Path,
        rows_by_partition: dict[str, list[dict]],
        schema: dict[str, str],
        backend_name: str = "csv",
        partition_column: str = "dt",
    ) -> Path:
        backend = get_backend(backend_name)
        table_dir = Path(table_dir)
        if table_dir.exists():
            shutil.rmtree(table_dir)
        for value, rows in sorted(rows_by_partition.items()):
            path = table_dir / f"{partition_column}={value}" / f"part-00000.{backend.extension}"
            backend.write_rows(path, rows, schema)
        return table_dir

    return _write
