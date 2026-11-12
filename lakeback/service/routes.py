"""HTTP route implementations. Every failure is converted to structured JSON."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs

from lakeback._util import partition_filters
from lakeback.backfill.report import load_report
from lakeback.config import Config
from lakeback.errors import NotFoundError, RequestError
from lakeback.snapshot.ledger import Ledger
from lakeback.travel.diff import diff_versions
from lakeback.travel.reader import read_view

STATIC_DIR = Path(__file__).resolve().parent / "static"


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes
    content_type: str


def json_response(payload: dict, status: int = 200) -> Response:
    body = (json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    return Response(status, body, "application/json; charset=utf-8")


def index_response() -> Response:
    try:
        body = (STATIC_DIR / "index.html").read_bytes()
    except OSError as exc:  # pragma: no cover - packaging failure
        raise NotFoundError(f"query page is unavailable: {exc}") from exc
    return Response(200, body, "text/html; charset=utf-8")


def _single(query: dict[str, list[str]], name: str, *, required: bool = True) -> str | None:
    values = query.get(name)
    if not values or not values[-1].strip():
        if required:
            raise RequestError(f"query parameter {name!r} is required")
        return None
    return values[-1].strip()


def _integer(query: dict[str, list[str]], name: str, default: int) -> int:
    raw = _single(query, name, required=False)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RequestError(f"query parameter {name!r} must be an integer") from exc
    if value < 0:
        raise RequestError(f"query parameter {name!r} must be non-negative")
    return value


def api_tables(config: Config) -> Response:
    ledger = Ledger(config.ledger_dir)
    tables = []
    for table, entry in ledger.tables().items():
        tables.append({
            "name": table,
            "latest_version": entry["latest_version"],
            "snapshots": entry["snapshots"],
        })
    return json_response({
        "backend": config.backend,
        "tables": tables,
        "table_count": len(tables),
    })


def api_travel(config: Config, query: dict[str, list[str]]) -> Response:
    table = _single(query, "table")
    at = _single(query, "at")
    try:
        partitions = partition_filters(query.get("partition", []))
    except ValueError as exc:
        raise RequestError(str(exc)) from exc
    limit = min(_integer(query, "limit", 50), 500)
    offset = _integer(query, "offset", 0)
    ledger = Ledger(config.ledger_dir)
    payload = read_view(
        config, ledger, table, at, partitions=partitions, limit=limit, offset=offset
    )
    return json_response(payload)


def api_diff(config: Config, query: dict[str, list[str]]) -> Response:
    table = _single(query, "table")
    from_at = _single(query, "from")
    to_at = _single(query, "to")
    ledger = Ledger(config.ledger_dir)
    return json_response(diff_versions(config, ledger, table, from_at, to_at))


def api_report(config: Config, query: dict[str, list[str]]) -> Response:
    report_id = _single(query, "report_id")
    return json_response(load_report(config.reports_dir, report_id))


def dispatch(config: Config, path: str, query_string: str) -> Response:
    if path in {"/", "/index.html"}:
        return index_response()
    query = parse_qs(query_string, keep_blank_values=True)
    if path == "/api/tables":
        return api_tables(config)
    if path == "/api/travel":
        return api_travel(config, query)
    if path == "/api/diff":
        return api_diff(config, query)
    if path == "/api/backfill/report":
        return api_report(config, query)
    raise NotFoundError(f"unknown route: {path}")
