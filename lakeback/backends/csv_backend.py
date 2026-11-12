"""Standard-library CSV backend used as the no-pyarrow fallback."""

from __future__ import annotations

import csv
from pathlib import Path

from lakeback._util import infer_type
from lakeback.backends.base import Backend, Schema, normalise_row
from lakeback.errors import BackendError, DataError


class CsvBackend(Backend):
    name = "csv"
    extension = "csv"
    availability_hint = "always available (Python standard library)"

    @classmethod
    def available(cls) -> bool:
        return True

    def read_rows(self, path: Path, schema: Schema | None = None) -> list[dict]:
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames is None:
                    raise DataError(f"CSV file has no header: {path}")
                names = list(reader.fieldnames)
                if schema is None:
                    raw_rows = [dict(row) for row in reader]
                    inferred = self._infer_from_raw(raw_rows, names)
                    return [normalise_row(row, inferred) for row in raw_rows]
                missing = set(schema) - set(names)
                if missing:
                    raise DataError(
                        f"CSV file {path} is missing columns: {', '.join(sorted(missing))}"
                    )
                return [normalise_row(row, schema) for row in reader]
        except (OSError, csv.Error) as exc:
            raise BackendError(f"cannot read CSV file {path}: {exc}") from exc

    def write_rows(self, path: Path, rows: list[dict], schema: Schema) -> None:
        if not schema:
            raise DataError("CSV writer requires a non-empty schema")
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(schema), lineterminator="\n")
                writer.writeheader()
                for row in rows:
                    writer.writerow({name: self._cell(row.get(name), schema[name]) for name in schema})
        except (OSError, csv.Error) as exc:
            raise BackendError(f"cannot write CSV file {path}: {exc}") from exc

    def infer_schema(self, path: Path) -> Schema:
        rows = self.read_rows(path, schema=None)
        if not rows:
            try:
                with path.open("r", encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle)
                    return {name: "str" for name in (reader.fieldnames or [])}
            except OSError as exc:
                raise BackendError(f"cannot inspect CSV file {path}: {exc}") from exc
        return {name: infer_type(row.get(name) for row in rows) for name in rows[0]}

    @staticmethod
    def _cell(value, kind: str) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        if kind == "float":
            return repr(float(value))
        return str(value)

    @staticmethod
    def _infer_from_raw(rows: list[dict], names: list[str]) -> Schema:
        def typed(value: str):
            if value == "":
                return None
            lowered = value.lower()
            if lowered in {"true", "false"}:
                return lowered == "true"
            try:
                return int(value)
            except ValueError:
                pass
            try:
                return float(value)
            except ValueError:
                return value

        schema: Schema = {}
        for name in names:
            values = [typed(row.get(name, "")) for row in rows]
            kind = infer_type(values)
            schema[name] = kind
        return schema

    def read_table(self, table_dir: Path, schema: Schema | None = None,
                   partitions=None) -> list[dict]:
        rows: list[dict] = []
        for entry in self.scan(table_dir, partitions):
            rows.extend(self.read_rows(entry.path, schema))
        return rows
