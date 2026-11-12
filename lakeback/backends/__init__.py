"""Backend registry. Imports stay lightweight so csv works without pyarrow."""

from __future__ import annotations

from lakeback.backends.base import Backend, DataFile, Schema
from lakeback.backends.csv_backend import CsvBackend
from lakeback.backends.parquet_backend import ParquetBackend
from lakeback.errors import ConfigError

_BACKENDS: dict[str, type[Backend]] = {
    CsvBackend.name: CsvBackend,
    ParquetBackend.name: ParquetBackend,
}


def get_backend(name: str) -> Backend:
    try:
        factory = _BACKENDS[name.lower()]
    except KeyError as exc:
        raise ValueError(
            f"unknown backend {name!r}; available: {', '.join(sorted(_BACKENDS))}"
        ) from exc
    return factory()


def available_backends() -> dict[str, bool]:
    return {name: factory.available() for name, factory in sorted(_BACKENDS.items())}


def backend_for_extension(extension: str) -> Backend:
    wanted = extension.lower().lstrip(".")
    for factory in _BACKENDS.values():
        if factory.extension == wanted:
            candidate = factory()
            if not candidate.available():
                raise ConfigError(
                    f"files require the {candidate.name!r} backend: {candidate.availability_hint}"
                )
            return candidate
    raise ConfigError(f"no backend handles file extension {extension!r}")


__all__ = [
    "Backend",
    "CsvBackend",
    "DataFile",
    "ParquetBackend",
    "Schema",
    "available_backends",
    "backend_for_extension",
    "get_backend",
]
