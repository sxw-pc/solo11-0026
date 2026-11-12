"""Optional pyarrow Parquet backend."""

from __future__ import annotations

from pathlib import Path

from lakeback.backends.base import Backend, Schema, coerce_value, normalise_row
from lakeback.errors import BackendError, DataError

try:  # The package remains importable when the optional extra is absent.
    import pyarrow as pa
    import pyarrow.parquet as pq

    PARQUET_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - exercised in the csv-only job
    pa = None
    pq = None
    PARQUET_AVAILABLE = False

_ARROW_TYPES = {
    "int": "int64",
    "float": "float64",
    "str": "string",
    "bool": "bool_",
}
_TYPE_ALIASES = {
    "int8": "int", "int16": "int", "int32": "int", "int64": "int",
    "uint8": "int", "uint16": "int", "uint32": "int", "uint64": "int",
    "float16": "float", "float32": "float", "float64": "float",
    "double": "float", "halffloat": "float", "string": "str", "large_string": "str",
    "bool": "bool", "boolean": "bool", "null": "null",
}
_TYPE_RANK = {"null": 0, "bool": 1, "int": 2, "float": 3, "str": 4}


class ParquetBackend(Backend):
    name = "parquet"
    extension = "parquet"
    availability_hint = "install pyarrow>=14 (pip install -e '.[parquet]')"

    @classmethod
    def available(cls) -> bool:
        return PARQUET_AVAILABLE and pa is not None and pq is not None

    def _require(self) -> None:
        if not self.available():  # pragma: no cover - depends on environment
            raise BackendError(
                "parquet backend unavailable: install pyarrow>=14 or switch to --backend csv"
            )

    def read_rows(self, path: Path, schema: Schema | None = None) -> list[dict]:
        self._require()
        try:
            table = pq.read_table(path)
            names = table.column_names
            if schema is None:
                inferred = self._schema_from_arrow(table.schema)
                return [normalise_row(row, inferred) for row in table.to_pylist()]
            missing = set(schema) - set(names)
            if missing:
                raise DataError(
                    f"Parquet file {path} is missing columns: {', '.join(sorted(missing))}"
                )
            return [normalise_row(row, schema) for row in table.to_pylist()]
        except (OSError, pa.ArrowException) as exc:
            raise BackendError(f"cannot read Parquet file {path}: {exc}") from exc

    def write_rows(self, path: Path, rows: list[dict], schema: Schema) -> None:
        self._require()
        if not schema:
            raise DataError("Parquet writer requires a non-empty schema")
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            arrays = []
            fields = []
            for name, kind in schema.items():
                arrow_type = _ARROW_TYPES.get(kind)
                if arrow_type is None:
                    raise DataError(f"Parquet backend cannot store type {kind!r}")
                values = [coerce_value(row.get(name), kind) for row in rows]
                arrays.append(pa.array(values, type=getattr(pa, arrow_type)()))
                fields.append(pa.field(name, getattr(pa, arrow_type)()))
            table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
            pq.write_table(table, path, compression="none", write_statistics=False)
        except (OSError, pa.ArrowException) as exc:
            raise BackendError(f"cannot write Parquet file {path}: {exc}") from exc

    def infer_schema(self, path: Path) -> Schema:
        self._require()
        try:
            return self._schema_from_arrow(pq.read_schema(path))
        except (OSError, pa.ArrowException) as exc:
            raise BackendError(f"cannot inspect Parquet file {path}: {exc}") from exc

    @staticmethod
    def _schema_from_arrow(arrow_schema) -> Schema:
        schema: Schema = {}
        for field in arrow_schema:
            raw = str(field.type).lower()
            kind = _TYPE_ALIASES.get(raw, "str")
            schema[field.name] = kind
        return schema
