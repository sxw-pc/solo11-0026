"""Command-line interface for LakeBack."""

from __future__ import annotations

import argparse
import json
import sys

from lakeback._util import partition_filters
from lakeback.backfill.auditor import run_audit
from lakeback.cli_diagnostics import cmd_doctor, cmd_selftest
from lakeback.cli_render import (
    render_audit,
    render_diff,
    render_generated,
    render_snapshot,
    render_travel,
)
from lakeback.config import Config
from lakeback.errors import LakeBackError, RequestError
from lakeback.fixtures.generate import generate_fixtures
from lakeback.service.app import make_server
from lakeback.snapshot.ledger import Ledger
from lakeback.snapshot.snapshotter import create_snapshot
from lakeback.travel.diff import diff_versions
from lakeback.travel.reader import read_view


def _add_output(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")


def _backend_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--backend", choices=["parquet", "csv"], default=None)


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be in 1..65535")
    return port


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lakeback",
        description="Offline lakehouse snapshot ledger, time travel and backfill auditor",
    )
    parser.add_argument("--config", default=None, help="path to lakeback.toml")
    subparsers = parser.add_subparsers(dest="command")

    gen = subparsers.add_parser("gen-fixtures", help="generate deterministic before/after fixtures")
    _add_output(gen)
    _backend_arg(gen)
    gen.add_argument("--seed", default=None, help="override seed TOML path")

    snapshot = subparsers.add_parser("snapshot", help="snapshot a local lake table directory")
    _add_output(snapshot)
    snapshot.add_argument("--table", required=True)
    snapshot.add_argument("--table-root", default=None)
    _backend_arg(snapshot)

    travel = subparsers.add_parser("travel", help="read a table at a snapshot version or time")
    _add_output(travel)
    travel.add_argument("--table", required=True)
    travel.add_argument("--at", required=True, help="version number or ISO-8601 timestamp")
    travel.add_argument("--partition", action="append", default=[], help="filter name=value")
    travel.add_argument("--limit", type=int, default=None)
    travel.add_argument("--offset", type=int, default=0)

    diff = subparsers.add_parser("diff", help="compare two snapshots by primary key")
    _add_output(diff)
    diff.add_argument("--table", required=True)
    diff.add_argument("--from", dest="from_at", required=True)
    diff.add_argument("--to", dest="to_at", required=True)

    audit = subparsers.add_parser("backfill-audit", help="audit two local backfill outputs")
    _add_output(audit)
    audit.add_argument("--table", required=True)
    audit.add_argument("--before", required=True)
    audit.add_argument("--after", required=True)
    audit.add_argument("--key", default=None)
    audit.add_argument(
        "--expectations",
        "--expected-changes",
        dest="expectations",
        default=None,
        metavar="PATH",
        help="TOML/JSON declaration of expected field changes",
    )
    _backend_arg(audit)

    serve = subparsers.add_parser("serve", help="run the read-only loopback HTTP service")
    serve.add_argument("--host", default=None)
    serve.add_argument("--port", type=_port, default=None)

    doctor = subparsers.add_parser("doctor", help="print environment diagnostics")
    _add_output(doctor)

    selftest = subparsers.add_parser("selftest", help="run a quick local backend self-test")
    _add_output(selftest)
    _backend_arg(selftest)
    return parser


def _emit(args, result: dict, renderer) -> None:
    if args.json:
        print(json.dumps(result, sort_keys=True, indent=2, ensure_ascii=False))
    else:
        print(renderer(result))


def _parse_partitions(raw: list[str]) -> dict[str, str]:
    try:
        return partition_filters(raw)
    except ValueError as exc:
        raise RequestError(str(exc)) from exc


def cmd_gen_fixtures(config: Config, args) -> int:
    result = generate_fixtures(config, backend_name=args.backend, seed_path=args.seed)
    _emit(args, result, render_generated)
    return 0


def cmd_snapshot(config: Config, args) -> int:
    manifest = create_snapshot(
        config, args.table, table_root=args.table_root, backend_name=args.backend
    )
    _emit(args, manifest, render_snapshot)
    return 0


def cmd_travel(config: Config, args) -> int:
    if args.limit is not None and args.limit < 0:
        raise RequestError("--limit must be non-negative")
    if args.offset < 0:
        raise RequestError("--offset must be non-negative")
    partitions = _parse_partitions(args.partition)
    result = read_view(
        config, Ledger(config.ledger_dir), args.table, args.at,
        partitions=partitions, limit=args.limit, offset=args.offset,
    )
    _emit(args, result, render_travel)
    return 0


def cmd_diff(config: Config, args) -> int:
    result = diff_versions(
        config, Ledger(config.ledger_dir), args.table, args.from_at, args.to_at
    )
    _emit(args, result, render_diff)
    return 0


def cmd_backfill_audit(config: Config, args) -> int:
    report = run_audit(
        config, args.table, args.before, args.after,
        key=args.key, backend_name=args.backend,
        expected_changes=args.expectations,
    )
    _emit(args, report, render_audit)
    if report["status"] != "consistent":
        print(
            f"lakeback: audit status={report['status']} report={report['report_id']}",
            file=sys.stderr,
        )
        return 4
    return 0


def cmd_serve(config: Config, args) -> int:
    server = make_server(config, host=args.host, port=args.port)
    host, port = server.server_address[:2]
    print(f"lakeback: serving read-only API on http://{host}:{port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("lakeback: shutting down", file=sys.stderr)
    finally:
        server.server_close()
    return 0


COMMANDS = {
    "gen-fixtures": cmd_gen_fixtures,
    "snapshot": cmd_snapshot,
    "travel": cmd_travel,
    "diff": cmd_diff,
    "backfill-audit": cmd_backfill_audit,
    "serve": cmd_serve,
    "doctor": cmd_doctor,
    "selftest": cmd_selftest,
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        # Hard-constraint convenience: ``python -m lakeback`` starts the query service.
        args.command = "serve"
        args.host = None
        args.port = None
    try:
        config = Config.load(args.config)
        return int(COMMANDS[args.command](config, args) or 0)
    except LakeBackError as exc:
        if getattr(args, "json", False):
            payload = {
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details,
                }
            }
            print(json.dumps(payload, sort_keys=True, indent=2), file=sys.stderr)
        else:
            print(f"lakeback: {exc.message}", file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
