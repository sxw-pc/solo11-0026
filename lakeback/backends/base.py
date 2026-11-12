"""Backend contract and shared partition-aware file discovery."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from lakeback._util import sha256_file
from lakeback.errors import DataError, NotFoundError, BackendError

Schema = dict[str, str]


@dataclass(frozen=True)
class DataFile:
    """One physical data file discovered below a local lake table directory."""

    path: Path
    relative: str
    partition: dict[str, str]

    def public(self) -> dict:
        return {"path": self.relative, "partition": dict(self.partition)}


def derive_partition(relative: str) -> dict[str, str]:
    """Read Hive-style ``name=value`` directory segments from a relative path."""
    partition: dict[str, str] = {}
    for segment in Path(relative).parts[:-1]:
        if "=" not in segment:
            continue
        name, _, value = segment.partition("=")
        if not name or not value:
            raise DataError(f"malformed partition directory segment: {segment!r}")
        if name in partition:
            raise DataError(f"duplicate partition column in path: {name!r}")
        partition[name] = value
    return partition


def matches_partition(partition: Mapping[str, str], filters: Mapping[str, str] | None) -> bool:
    if not filters:
        return True
    return all(partition.get(name) == value for name, value in filters.items())


def coerce_value(value: Any, target: str) -> Any:
    """Normalise values to the small portable type system used in manifests."""
    if value is None or value == "":
        return None
    if target == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1"}:
                return True
            if lowered in {"false", "0"}:
                return False
            raise DataError(f"cannot parse boolean value {value!r}")
        return bool(value)
    if target == "int":
        if isinstance(value, bool):
            raise DataError(f"cannot parse integer value {value!r}")
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise DataError(f"cannot parse integer value {value!r}") from exc
    if target == "float":
        if isinstance(value, bool):
            raise DataError(f"cannot parse float value {value!r}")
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise DataError(f"cannot parse float value {value!r}") from exc
    if target == "str":
        return str(value)
    if target == "null":
        return None
    raise DataError(f"unsupported schema type: {target!r}")


def normalise_row(row: Mapping[str, Any], schema: Schema) -> dict:
    return {name: coerce_value(row.get(name), kind) for name, kind in schema.items()}


class Backend(ABC):
    """Minimal read/write/scan interface implemented by csv and parquet."""

    name = "base"
    extension = ""
    availability_hint = ""

    @classmethod
    @abstractmethod
    def available(cls) -> bool:
        """Whether this backend's optional runtime dependency is importable."""

    @abstractmethod
    def read_rows(self, path: Path, schema: Schema | None = None) -> list[dict]:
        """Read one file as dictionaries in deterministic file order."""

    @abstractmethod
    def write_rows(self, path: Path, rows: list[dict], schema: Schema) -> None:
        """Write rows with an explicit ordered schema."""

    @abstractmethod
    def infer_schema(self, path: Path) -> Schema:
        """Infer an ordered portable schema from one file."""

    def checksum(self, path: Path) -> str:
        return sha256_file(path)

    def scan(self, table_dir: Path, partitions: Mapping[str, str] | None = None) -> list[DataFile]:
        """Discover this backend's files, deterministic by relative path."""
        root = table_dir.resolve()
        if not root.exists():
            raise NotFoundError(f"table directory does not exist: {table_dir}")
        if not root.is_dir():
            raise DataError(f"table path is not a directory: {table_dir}")
        files: list[DataFile] = []
        try:
            candidates = sorted(
                (p for p in root.rglob(f"*.{self.extension}") if p.is_file()),
                key=lambda p: p.relative_to(root).as_posix(),
            )
        except OSError as exc:
            raise BackendError(f"cannot scan table directory {table_dir}: {exc}") from exc
        for path in candidates:
            relative = path.relative_to(root).as_posix()
            partition = derive_partition(relative)
            if matches_partition(partition, partitions):
                files.append(DataFile(path=path, relative=relative, partition=partition))
        return files

    def read_table(
        self, table_dir: Path, schema: Schema | None = None,
        partitions: Mapping[str, str] | None = None,
    ) -> list[dict]:
        rows: list[dict] = []
        for entry in self.scan(table_dir, partitions):
            rows.extend(self.read_rows(entry.path, schema))
        return rows
