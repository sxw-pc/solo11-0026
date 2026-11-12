"""Loopback-only read-only HTTP server assembled from the standard library."""

from __future__ import annotations

import socket
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from lakeback.config import Config, validate_host
from lakeback.errors import LakeBackError, NotFoundError, RequestError
from lakeback.service.routes import Response, dispatch, json_response

LOOPBACK = {"127.0.0.1", "::1", "localhost"}


class LakeBackHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, config: Config):
        self.config = config
        super().__init__(address, RequestHandler)


class LakeBackHTTPServerV6(LakeBackHTTPServer):
    address_family = socket.AF_INET6


class RequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "LakeBack/0.1"

    def _send(self, response: Response, head_only: bool = False) -> None:
        self.send_response(response.status)
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(response.body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        if not head_only:
            self.wfile.write(response.body)

    def _error(self, status: int, code: str, message: str, details: dict | None = None) -> Response:
        error = {"code": code, "message": message, "status": status}
        if details:
            error["details"] = details
        return json_response({"error": error}, status=status)

    def _handle(self, head_only: bool = False) -> None:
        parsed = urlsplit(self.path)
        try:
            response = dispatch(self.server.config, parsed.path, parsed.query)
        except RequestError as exc:
            response = self._error(400, exc.code, exc.message, exc.details)
        except NotFoundError as exc:
            response = self._error(404, exc.code, exc.message, exc.details)
        except LakeBackError as exc:
            response = self._error(400, exc.code, exc.message, exc.details)
        except Exception as exc:  # pragma: no cover - last-resort safety net
            traceback.print_exc(file=sys.stderr)
            response = self._error(500, "internal_error", str(exc))
        self._send(response, head_only=head_only)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        self._handle()

    def do_HEAD(self) -> None:  # noqa: N802
        self._handle(head_only=True)

    def _reject_write(self) -> None:
        response = self._error(405, "method_not_allowed", "this service is read-only")
        self.send_response(405)
        self.send_header("Allow", "GET, HEAD")
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(response.body)))
        self.end_headers()
        self.wfile.write(response.body)

    do_POST = _reject_write  # noqa: N815
    do_PUT = _reject_write  # noqa: N815
    do_PATCH = _reject_write  # noqa: N815
    do_DELETE = _reject_write  # noqa: N815

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("lakeback http: " + (fmt % args) + "\n")


def make_server(config: Config, host: str | None = None, port: int | None = None) -> LakeBackHTTPServer:
    """Build a server bound strictly to loopback; port 0 is useful for tests."""
    selected_host = validate_host(host or config.host)
    selected_port = config.port if port is None else port
    if not isinstance(selected_port, int) or not 0 <= selected_port <= 65535:
        raise RequestError("port must be an integer in 0..65535")
    if selected_host not in LOOPBACK:
        raise RequestError(f"refusing non-loopback host: {selected_host!r}")
    config.resolve_backend()
    config.ensure_runtime_dirs()
    factory = LakeBackHTTPServerV6 if selected_host == "::1" else LakeBackHTTPServer
    try:
        return factory((selected_host, selected_port), config)
    except OSError as exc:
        raise RequestError(
            f"cannot bind {selected_host}:{selected_port}: {exc}",
            details={"hint": "choose another port with --port"},
        ) from exc
