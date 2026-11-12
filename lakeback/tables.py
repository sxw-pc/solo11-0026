"""Shared discovery and typed reading for a local lake table directory."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from lakeback._util import partition_tuple
from lakeback.backends.base import Backend, DataFile, Schema
from lakeback.errors import DataError, NotFoundError


@dataclass
class TableData:
    table_dir: Path
    schema: Schema
    rows: list[dict]
    entries: list[DataFile]
    partitions: list[dict]
    partition_columns: list[str]
    rows_by_partition: dict[tuple[tuple[str, str], ...], list[dict]]

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def files(self) -> list[str]:
        return [entry.relative for entry in self.entries]


def merge_types(left: str, right: str) -> str:
    if left == right:
        return left
    if left == "null":
        return right
    if right == "null":
        return left
    if {left, right} <= {"int", "float"}:
        return "float"
    return "str"


def merge_schemas(schemas: list[Schema]) -> Schema:
    if not schemas:
        return {}
    merged = dict(schemas[0])
    expected = set(merged)
    for schema in schemas[1:]:
        if set(schema) != expected:
            missing = sorted(expected - set(schema))
            extra = sorted(set(schema) - expected)
            raise DataError(
                "table files have inconsistent columns",
                details={"missing": missing, "extra": extra},
            )
        for name in merged:
            merged[name] = merge_types(merged[name], schema[name])
    return merged


def discover_schema(
    backend: Backend, table_dir: Path, partitions: Mapping[str, str] | None = None
) -> tuple[Schema, list[DataFile]]:
    entries = backend.scan(table_dir, partitions)
    if not entries:
        raise NotFoundError(
            f"no {backend.name} data files under table directory: {table_dir}",
            details={"backend": backend.name},
        )
    schemas = [backend.infer_schema(entry.path) for entry in entries]
    schema = merge_schemas(schemas)
    if not schema:
        raise DataError(f"table schema is empty: {table_dir}")
    return schema, entries


def _partition_layout(entries: list[DataFile]) -> tuple[list[str], list[dict]]:
    layouts = {tuple(sorted(entry.partition)) for entry in entries}
    if len(layouts) != 1:
        raise DataError(
            "table files use inconsistent partition directory layouts",
            details={"layouts": [list(layout) for layout in sorted(layouts)]},
        )
    columns = sorted(layouts.pop())
    unique: dict[tuple, dict] = {}
    for entry in entries:
        unique[partition_tuple(entry.partition)] = dict(entry.partition)
    return columns, [unique[key] for key in sorted(unique)]


def read_table_data(
    backend: Backend,
    table_dir: Path,
    schema: Schema | None = None,
    partitions: Mapping[str, str] | None = None,
) -> TableData:
    """Read every matching file into memory once, with a stable schema."""
    resolved = table_dir.resolve()
    if schema is None:
        schema, entries = discover_schema(backend, resolved, partitions)
    else:
        entries = backend.scan(resolved, partitions)
        if not entries:
            raise NotFoundError(f"no data files under table directory: {table_dir}")
    rows: list[dict] = []
    grouped: dict[tuple[tuple[str, str], ...], list[dict]] = {}
    for entry in entries:
        file_rows = backend.read_rows(entry.path, schema)
        rows.extend(file_rows)
        marker = partition_tuple(entry.partition)
        grouped.setdefault(marker, []).extend(file_rows)
    partition_columns, partition_values = _partition_layout(entries)
    return TableData(
        table_dir=resolved,
        schema=schema,
        rows=rows,
        entries=entries,
        partitions=partition_values,
        partition_columns=partition_columns,
        rows_by_partition=grouped,
    )
