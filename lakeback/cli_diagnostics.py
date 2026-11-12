"""Doctor and selftest commands, separated to keep the CLI module small."""

from __future__ import annotations

import json
import platform
import tempfile
from pathlib import Path

from lakeback import __version__
from lakeback._util import tree_digest
from lakeback.backends import available_backends
from lakeback.cli_render import render_doctor, render_selftest
from lakeback.config import Config
from lakeback.errors import BackendError, LakeBackError
from lakeback.snapshot.ledger import Ledger


def _emit(args, result: dict, renderer) -> None:
    if args.json:
        print(json.dumps(result, sort_keys=True, indent=2, ensure_ascii=False))
    else:
        print(renderer(result))


def _doctor_result(config: Config) -> tuple[dict, bool]:
    backends = available_backends()
    try:
        import pyarrow

        pyarrow_version = pyarrow.__version__
    except ModuleNotFoundError:  # pragma: no cover - environment dependent
        pyarrow_version = None
    ledger_path = config.ledger_dir / "snapshots.jsonl"
    if ledger_path.exists():
        try:
            ledger_info = {
                "path": str(ledger_path),
                "records": len(Ledger(config.ledger_dir).snapshots()),
                "status": "ok",
            }
        except LakeBackError as exc:
            ledger_info = {"path": str(ledger_path), "status": "error", "error": exc.message}
    else:
        ledger_info = {"path": str(ledger_path), "records": 0, "status": "not_initialised"}
    directories = {
        name: str(path)
        for name, path in {
            "lake": config.lake_root,
            "ledger": config.ledger_dir,
            "snapshots": config.snapshot_store,
            "reports": config.reports_dir,
            "fixtures": config.fixtures_dir,
        }.items()
    }
    available = bool(backends.get(config.backend))
    ok = available and config.seed_path.is_file()
    result = {
        "version": __version__,
        "python": platform.python_version(),
        "pyarrow": pyarrow_version,
        "backends": backends,
        "configured_backend": config.backend,
        "configured_backend_available": available,
        "config": str(config.path),
        "seed_path": str(config.seed_path),
        "seed_exists": config.seed_path.is_file(),
        "service": {"host": config.host, "port": config.port},
        "ledger": ledger_info,
        "directories": directories,
        "ok": ok,
    }
    if not available:
        result["hint"] = (
            "install pyarrow>=14 with pip install -e '.[parquet]', "
            "or set storage.backend = \"csv\""
        )
    return result, ok


def cmd_doctor(config: Config, args) -> int:
    result, ok = _doctor_result(config)
    _emit(args, result, render_doctor)
    return 0 if ok else 2


def cmd_selftest(config: Config, args) -> int:
    backend = config.resolve_backend(args.backend)
    checks = {}
    with tempfile.TemporaryDirectory(prefix="lakeback-selftest-") as temporary:
        root = Path(temporary)
        schema = {"id": "int", "name": "str", "active": "bool"}
        rows = [
            {"id": 1, "name": "alpha", "active": True},
            {"id": 2, "name": "beta", "active": False},
        ]
        path = root / "dt=2024-01-01" / f"part-00000.{backend.extension}"
        backend.write_rows(path, rows, schema)
        checks["write"] = "ok" if path.is_file() else "failed"
        loaded = backend.read_rows(path, schema)
        checks["read_roundtrip"] = "ok" if loaded == rows else "failed"
        checks["checksum"] = "ok" if len(backend.checksum(path)) == 64 else "failed"
        checks["scan"] = "ok" if len(backend.scan(root)) == 1 else "failed"
        checks["tree_digest"] = "ok" if len(tree_digest(root)) == 64 else "failed"
        result = {
            "backend": backend.name,
            "temporary": temporary,
            "checks": checks,
            "ok": all(value == "ok" for value in checks.values()),
        }
    _emit(args, result, render_selftest)
    if not result["ok"]:
        raise BackendError("selftest failed; see checks above")
    return 0
