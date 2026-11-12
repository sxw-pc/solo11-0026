"""Deterministically generate synthetic before/after lake table directories."""

from __future__ import annotations

import random
import re
import shutil
from pathlib import Path
from typing import Any

from lakeback._toml import TOMLDecodeError, load as load_toml
from lakeback._util import NAME_RE, primary_key_sort, tree_digest
from lakeback.config import Config
from lakeback.errors import ConfigError

TYPES = {"int", "float", "str", "bool"}
PART_VALUE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def _load_seed(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"seed file not found: {path}")
    try:
        with path.open("rb") as handle:
            raw = load_toml(handle)
    except (OSError, TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read seed file {path}: {exc}") from exc
    if not isinstance(raw.get("seed"), int) or raw["seed"] < 0:
        raise ConfigError("seed file must define a non-negative integer 'seed'")
    partitioning = raw.get("partitioning")
    if not isinstance(partitioning, dict):
        raise ConfigError("seed file must define [partitioning]")
    column = partitioning.get("column")
    values = partitioning.get("values")
    if not isinstance(column, str) or not isinstance(values, list) or not values:
        raise ConfigError("[partitioning] needs a column and non-empty values")
    if len(set(values)) != len(values):
        raise ConfigError("[partitioning].values must be unique")
    for value in values:
        if not isinstance(value, str) or not PART_VALUE_RE.fullmatch(value):
            raise ConfigError(f"unsafe partition value in seed file: {value!r}")
    tables = raw.get("tables")
    if not isinstance(tables, dict) or not tables:
        raise ConfigError("seed file must define at least one [tables.<name>]")
    for table, spec in tables.items():
        _validate_table(table, spec, column)
    backfill = raw.get("backfill", {})
    if not isinstance(backfill, dict):
        raise ConfigError("[backfill] must be a table")
    if backfill:
        target = backfill.get("table")
        if target not in tables:
            raise ConfigError("[backfill].table must name a configured table")
        for key in ("modify_rows", "delete_rows", "add_rows"):
            value = backfill.get(key, 0)
            if not isinstance(value, int) or value < 0:
                raise ConfigError(f"[backfill].{key} must be a non-negative integer")
        if backfill.get("modify_rows", 0) + backfill.get("delete_rows", 0) > tables[target]["rows"]:
            raise ConfigError("[backfill] cannot modify/delete more rows than the table has")
    return raw


def _validate_table(table: str, spec: Any, partition_column: str) -> None:
    if not NAME_RE.fullmatch(table) or not isinstance(spec, dict):
        raise ConfigError(f"invalid table entry in seed: {table!r}")
    if not isinstance(spec.get("rows"), int) or spec["rows"] < 1:
        raise ConfigError(f"[tables.{table}].rows must be a positive integer")
    columns = spec.get("columns")
    if not isinstance(columns, list) or not columns:
        raise ConfigError(f"[tables.{table}] needs columns")
    seen: set[str] = set()
    for item in columns:
        if not isinstance(item, dict):
            raise ConfigError(f"[tables.{table}].columns entries must be tables")
        name, kind = item.get("name"), item.get("type")
        if not isinstance(name, str) or not name or name in seen:
            raise ConfigError(f"[tables.{table}] contains an invalid or duplicate column")
        if kind not in TYPES:
            raise ConfigError(f"[tables.{table}].{name} has unsupported type {kind!r}")
        seen.add(name)
    if spec.get("key") not in seen:
        raise ConfigError(f"[tables.{table}].key must name a column")
    if partition_column not in seen:
        raise ConfigError(f"[tables.{table}] must include partition column {partition_column!r}")
    if not isinstance(spec.get("id_start"), int):
        raise ConfigError(f"[tables.{table}].id_start must be an integer")


def schema_for(spec: dict) -> dict[str, str]:
    return {item["name"]: item["type"] for item in spec["columns"]}


def _pick(column: str, kind: str, table: str, index: int, rng: random.Random,
          partition: str, context: dict) -> Any:
    if column == context["partition_column"]:
        return partition
    if column == context["key"]:
        return context["id_start"] + index
    if column == "user_id":
        return 5000 + rng.randrange(1, 300)
    if column == "region":
        return rng.choice(["north", "south", "east", "west"])
    if column == "tier":
        return rng.choice(["basic", "plus", "premium"])
    if column == "updated_at":
        return partition + "T00:00:00Z"
    if column == "amount":
        return round(rng.uniform(5.0, 900.0), 2)
    if column == "status":
        return rng.choice(["paid", "pending", "refunded"])
    if column == "reason":
        return rng.choice(["damaged", "late", "duplicate", "changed_mind"])
    if column == "order_id":
        return 100000 + rng.randrange(0, context["order_rows"])
    if kind == "int":
        return rng.randrange(1, 100000)
    if kind == "float":
        return round(rng.uniform(0.0, 100.0), 2)
    if kind == "bool":
        return rng.random() < 0.5
    return f"{table}-{column}-{index % 37}"


def generate_rows(table: str, spec: dict, seed: int, table_index: int,
                  partitions: list[str], partition_column: str,
                  order_rows: int) -> list[dict]:
    rng = random.Random(seed + table_index * 1009)
    schema = schema_for(spec)
    rows: list[dict] = []
    for index in range(spec["rows"]):
        partition = partitions[index % len(partitions)]
        context = {
            "partition_column": partition_column,
            "key": spec["key"],
            "id_start": spec["id_start"],
            "order_rows": order_rows,
        }
        rows.append({
            name: _pick(name, kind, table, index, rng, partition, context)
            for name, kind in schema.items()
        })
    return rows


def _mutate_rows(table: str, spec: dict, seed: int, table_index: int,
                 rows: list[dict], mutation: dict, partitions: list[str],
                 partition_column: str, order_rows: int) -> list[dict]:
    if mutation.get("table") != table:
        return rows
    schema = schema_for(spec)
    key = spec["key"]
    rng = random.Random(seed + 7001 + table_index)
    modify_count = mutation.get("modify_rows", 0)
    delete_count = mutation.get("delete_rows", 0)
    selected = rng.sample(range(len(rows)), modify_count + delete_count)
    deleted = set(selected[:delete_count])
    modified_indices = set(selected[delete_count:])
    result: list[dict] = []
    candidates = [
        name for name in schema if name != key and name != partition_column
    ]
    for index, row in enumerate(rows):
        if index in deleted:
            continue
        updated = dict(row)
        if index in modified_indices:
            for name in candidates:
                if schema[name] in {"int", "float"}:
                    updated[name] = updated[name] + (1.25 if schema[name] == "float" else 1)
                    break
                if schema[name] == "str":
                    updated[name] = "repaired-" + str(updated[name])
                    break
        result.append(updated)
    for offset in range(mutation.get("add_rows", 0)):
        index = spec["rows"] + offset
        partition = partitions[index % len(partitions)]
        context = {
            "partition_column": partition_column,
            "key": key,
            "id_start": spec["id_start"],
            "order_rows": order_rows,
        }
        result.append({
            name: _pick(name, kind, table, index, rng, partition, context)
            for name, kind in schema.items()
        })
    return result


def _group(rows: list[dict], partition_column: str, key: str) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for row in rows:
        groups.setdefault(str(row[partition_column]), []).append(row)
    for value in groups:
        groups[value].sort(key=lambda row: primary_key_sort(row[key]))
    return groups


def _write_table(table_root: Path, rows: list[dict], spec: dict, backend,
                 partition_column: str) -> None:
    schema = schema_for(spec)
    if table_root.exists():
        shutil.rmtree(table_root)
    groups = _group(rows, partition_column, spec["key"])
    for value in sorted(groups):
        path = table_root / f"{partition_column}={value}" / f"part-00000.{backend.extension}"
        backend.write_rows(path, groups[value], schema)


def _write_root(root: Path, table_sets: dict[str, list[dict]], seed_raw: dict,
                backend, partition_column: str) -> None:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    for table in sorted(table_sets):
        _write_table(root / table, table_sets[table], seed_raw["tables"][table],
                     backend, partition_column)


def generate_fixtures(config: Config, *, backend_name: str | None = None,
                      seed_path: str | Path | None = None) -> dict:
    """Rebuild fixtures and the current lake mirror from one seeded specification."""
    backend = config.resolve_backend(backend_name)
    path = Path(seed_path).resolve() if seed_path else config.seed_path
    seed_raw = _load_seed(path)
    seed = seed_raw["seed"]
    partition_column = seed_raw["partitioning"]["column"]
    partitions = seed_raw["partitioning"]["values"]
    order_rows = seed_raw["tables"].get("orders", {}).get("rows", 1)
    tables = sorted(seed_raw["tables"])
    before: dict[str, list[dict]] = {}
    after: dict[str, list[dict]] = {}
    schemas: dict[str, dict] = {}
    for index, table in enumerate(tables):
        spec = seed_raw["tables"][table]
        schemas[table] = schema_for(spec)
        base = generate_rows(
            table, spec, seed, index, partitions, partition_column, order_rows
        )
        before[table] = base
        after[table] = _mutate_rows(
            table, spec, seed, index, base, seed_raw.get("backfill", {}),
            partitions, partition_column, order_rows,
        )
    fixture_root = config.fixtures_dir
    if fixture_root == config.project_root or fixture_root == Path.home().resolve():
        raise ConfigError(f"refusing to rebuild unsafe fixtures directory: {fixture_root}")
    before_root = fixture_root / "before"
    after_root = fixture_root / "after"
    _write_root(before_root, before, seed_raw, backend, partition_column)
    _write_root(after_root, after, seed_raw, backend, partition_column)
    config.lake_root.mkdir(parents=True, exist_ok=True)
    for table in tables:
        destination = config.lake_root / table
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(after_root / table, destination)
    counts = {
        table: {"before_rows": len(before[table]), "after_rows": len(after[table])}
        for table in tables
    }
    return {
        "seed": seed,
        "seed_file": str(path),
        "backend": backend.name,
        "partition_column": partition_column,
        "partitions": partitions,
        "tables": {
            table: {**counts[table], "schema": schemas[table]} for table in tables
        },
        "paths": {
            "before": str(before_root),
            "after": str(after_root),
            "lake": str(config.lake_root),
        },
        "digests": {
            "before": tree_digest(before_root),
            "after": tree_digest(after_root),
            "lake": tree_digest(config.lake_root),
        },
    }
