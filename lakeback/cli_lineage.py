"""Lineage subcommands, kept out of the main CLI module like the diagnostics."""

from __future__ import annotations

import json

from lakeback.cli_render import (
    render_lineage_downstream,
    render_lineage_registered,
    render_lineage_upstream,
)
from lakeback.config import Config
from lakeback.errors import RequestError
from lakeback.lineage.graph import downstream, upstream
from lakeback.lineage.resolve import resolve_target
from lakeback.lineage.store import LineageStore, parse_source_spec
from lakeback.snapshot.ledger import Ledger


def _add_output(parser) -> None:
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")


def _identify(parser) -> None:
    parser.add_argument("--snapshot", default=None, help="snapshot id (snap-...)")
    parser.add_argument("--table", default=None, help="table name")
    parser.add_argument("--at", default=None, help="version, 'latest' or ISO-8601 time")


def add_lineage_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "lineage", help="register and query cross-table snapshot lineage"
    )
    sub = parser.add_subparsers(dest="lineage_command", required=True)

    register = sub.add_parser(
        "register", help="declare which upstream versions a snapshot was computed from"
    )
    _identify(register)
    register.add_argument(
        "--source", action="append", default=[], metavar="TABLE=VERSION",
        help="upstream table version read by this snapshot; repeatable; "
        "omit all sources to declare a source table",
    )
    _add_output(register)

    up = sub.add_parser("upstream", help="list the full upstream chain of a snapshot")
    _identify(up)
    _add_output(up)

    down = sub.add_parser(
        "downstream", help="list every snapshot depending on a table version, by depth"
    )
    _identify(down)
    down.add_argument("--limit", type=int, default=None, help="cap the number of entries shown")
    _add_output(down)


def _emit(args, result: dict, renderer) -> None:
    if args.json:
        print(json.dumps(result, sort_keys=True, indent=2, ensure_ascii=False))
    else:
        print(renderer(result))


def _target(ledger: Ledger, args) -> dict:
    return resolve_target(ledger, snapshot=args.snapshot, table=args.table, at=args.at)


def register_snapshot_lineage(
    config: Config, ledger: Ledger, snapshot_id: str, raw_sources
) -> dict:
    """Register lineage for a freshly created snapshot (used by ``snapshot --source``)."""
    sources = [parse_source_spec(raw) for raw in raw_sources]
    store = LineageStore(config.ledger_dir, ledger)
    return store.register(snapshot_id, sources, created_at=config.now_iso())


def cmd_lineage_register(config: Config, args) -> int:
    ledger = Ledger(config.ledger_dir)
    manifest = _target(ledger, args)
    result = register_snapshot_lineage(config, ledger, manifest["snapshot_id"], args.source)
    _emit(args, result, render_lineage_registered)
    return 0


def cmd_lineage_upstream(config: Config, args) -> int:
    ledger = Ledger(config.ledger_dir)
    manifest = _target(ledger, args)
    store = LineageStore(config.ledger_dir, ledger)
    _emit(args, upstream(ledger, store, manifest["snapshot_id"]), render_lineage_upstream)
    return 0


def cmd_lineage_downstream(config: Config, args) -> int:
    if args.limit is not None and args.limit < 0:
        raise RequestError("--limit must be non-negative")
    ledger = Ledger(config.ledger_dir)
    manifest = _target(ledger, args)
    store = LineageStore(config.ledger_dir, ledger)
    result = downstream(ledger, store, manifest["snapshot_id"], limit=args.limit)
    _emit(args, result, render_lineage_downstream)
    return 0


LINEAGE_COMMANDS = {
    "register": cmd_lineage_register,
    "upstream": cmd_lineage_upstream,
    "downstream": cmd_lineage_downstream,
}


def cmd_lineage(config: Config, args) -> int:
    return LINEAGE_COMMANDS[args.lineage_command](config, args)
