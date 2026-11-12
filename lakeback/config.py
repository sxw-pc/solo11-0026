"""Load and validate ``lakeback.toml`` without touching the network."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lakeback._toml import TOMLDecodeError, load as load_toml
from lakeback._util import parse_timestamp, to_iso, utc_now_iso
from lakeback.errors import ConfigError

LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _mapping(value: Any, section: str) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"config section [{section}] must be a table")
    return value


def _string(mapping: dict, key: str, default: str, section: str) -> str:
    value = mapping.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"config key [{section}].{key} must be a non-empty string")
    return value.strip()


def _path(mapping: dict, key: str, default: str, base: Path, section: str) -> Path:
    raw = _string(mapping, key, default, section)
    candidate = Path(raw)
    return candidate.resolve() if candidate.is_absolute() else (base / candidate).resolve()


def validate_host(host: str) -> str:
    """Only loopback binding is allowed anywhere in the CLI or service layer."""
    normalised = host.strip()
    if normalised == "localhost":
        return normalised
    try:
        address = ipaddress.ip_address(normalised)
    except ValueError as exc:
        raise ConfigError(
            f"service host {host!r} is not an IP address; only loopback is allowed"
        ) from exc
    if not address.is_loopback:
        raise ConfigError(
            f"refusing non-loopback service host {host!r}; use 127.0.0.1, ::1, or localhost"
        )
    return normalised


@dataclass(frozen=True)
class Config:
    """Validated, resolved process configuration."""

    path: Path
    project_root: Path
    lake_root: Path
    ledger_dir: Path
    snapshot_store: Path
    reports_dir: Path
    fixtures_dir: Path
    seed_path: Path
    backend: str = "parquet"
    host: str = "127.0.0.1"
    port: int = 5000
    clock_mode: str = "system"
    fixed_time: str | None = None
    table_keys: dict[str, str] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Config":
        config_path = Path(path).expanduser().resolve() if path else Path.cwd() / "lakeback.toml"
        if not config_path.is_file():
            raise ConfigError(
                f"configuration file not found: {config_path}",
                details={"hint": "run from the repository root or pass --config PATH"},
            )
        try:
            with config_path.open("rb") as handle:
                raw = load_toml(handle)
        except (OSError, TOMLDecodeError) as exc:
            raise ConfigError(f"cannot read configuration {config_path}: {exc}") from exc

        base = config_path.parent
        paths = _mapping(raw.get("paths"), "paths")
        storage = _mapping(raw.get("storage"), "storage")
        service = _mapping(raw.get("service"), "service")
        clock = _mapping(raw.get("clock"), "clock")
        keys = _mapping(raw.get("keys"), "keys")

        backend = _string(storage, "backend", "parquet", "storage").lower()
        if backend not in {"parquet", "csv"}:
            raise ConfigError(
                f"unsupported backend {backend!r}; choose 'parquet' or 'csv'",
                details={"backend": backend},
            )
        host = validate_host(_string(service, "host", "127.0.0.1", "service"))
        port = service.get("port", 5000)
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise ConfigError("config key [service].port must be an integer in 1..65535")

        clock_mode = _string(clock, "mode", "system", "clock").lower()
        if clock_mode not in {"system", "fixed"}:
            raise ConfigError("config key [clock].mode must be 'system' or 'fixed'")
        fixed_time = None
        if clock_mode == "fixed":
            fixed_raw = clock.get("fixed_time")
            if not isinstance(fixed_raw, str):
                raise ConfigError("[clock].fixed_time is required when mode = 'fixed'")
            try:
                fixed_time = to_iso(parse_timestamp(fixed_raw))
            except ValueError as exc:
                raise ConfigError(f"invalid [clock].fixed_time: {exc}") from exc

        table_keys: dict[str, str] = {}
        for table, value in keys.items():
            if not isinstance(table, str) or not isinstance(value, str) or not value:
                raise ConfigError("[keys] must map table names to non-empty field names")
            table_keys[table] = value

        return cls(
            path=config_path,
            project_root=base,
            lake_root=_path(paths, "lake", "data/lake", base, "paths"),
            ledger_dir=_path(paths, "ledger", "data/ledger", base, "paths"),
            snapshot_store=_path(paths, "snapshots", "data/snapshots", base, "paths"),
            reports_dir=_path(paths, "reports", "data/reports", base, "paths"),
            fixtures_dir=_path(paths, "fixtures", "data/fixtures", base, "paths"),
            seed_path=_path(paths, "seed", "lakeback/fixtures/seed.toml", base, "paths"),
            backend=backend,
            host=host,
            port=port,
            clock_mode=clock_mode,
            fixed_time=fixed_time,
            table_keys=table_keys,
        )

    def now_iso(self) -> str:
        """Return the configured clock; fixed clocks make demos reproducible."""
        return self.fixed_time if self.clock_mode == "fixed" else utc_now_iso()

    def resolve_backend(self, name: str | None = None):
        """Instantiate a backend, failing with an actionable fallback hint."""
        from lakeback.backends import get_backend

        selected = (name or self.backend).lower()
        try:
            backend = get_backend(selected)
        except ValueError as exc:
            raise ConfigError(str(exc), details={"backend": selected}) from exc
        if not backend.available():
            hint = (
                "install the 'parquet' extra (pyarrow>=14), or use "
                "--backend csv / set storage.backend = \"csv\""
                if selected == "parquet"
                else "the csv backend requires only the Python standard library"
            )
            raise ConfigError(
                f"backend {selected!r} is unavailable: {hint}",
                details={"backend": selected, "hint": hint},
            )
        return backend

    def table_key(self, table: str, override: str | None = None) -> str:
        key = override or self.table_keys.get(table)
        if not key:
            raise ConfigError(
                f"no primary key configured for table {table!r}",
                details={"hint": "add [keys].<table> to lakeback.toml or pass --key"},
            )
        return key

    def ensure_runtime_dirs(self) -> None:
        for directory in (
            self.lake_root,
            self.ledger_dir,
            self.snapshot_store,
            self.reports_dir,
        ):
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise ConfigError(f"cannot create runtime directory {directory}: {exc}") from exc

    def describe(self) -> dict:
        return {
            "config": str(self.path),
            "project_root": str(self.project_root),
            "backend": self.backend,
            "lake_root": str(self.lake_root),
            "ledger_dir": str(self.ledger_dir),
            "snapshot_store": str(self.snapshot_store),
            "reports_dir": str(self.reports_dir),
            "fixtures_dir": str(self.fixtures_dir),
            "seed_path": str(self.seed_path),
            "service": {"host": self.host, "port": self.port},
            "clock": {"mode": self.clock_mode, "fixed_time": self.fixed_time},
            "table_keys": dict(self.table_keys),
        }
