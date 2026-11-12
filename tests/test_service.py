from __future__ import annotations

import ipaddress
import json
import shutil
import threading
import urllib.error
import urllib.request

import pytest

from lakeback.backfill.auditor import run_audit
from lakeback.backfill.report import load_report
from lakeback.config import Config
from lakeback.errors import ConfigError
from lakeback.fixtures.generate import generate_fixtures
from lakeback.service.app import make_server
from lakeback.snapshot.snapshotter import create_snapshot


@pytest.fixture
def service(make_config):
    config = make_config()
    generate_fixtures(config)
    create_snapshot(config, "orders", clock=lambda: "2025-01-01T00:00:00Z")
    shutil.rmtree(config.lake_root / "orders")
    shutil.copytree(config.fixtures_dir / "before" / "orders", config.lake_root / "orders")
    create_snapshot(config, "orders", clock=lambda: "2025-01-02T00:00:00Z")
    report = run_audit(
        config, "orders",
        config.fixtures_dir / "before" / "orders",
        config.fixtures_dir / "after" / "orders",
    )
    server = make_server(config, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}", config, report
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _get(base, path):
    try:
        with urllib.request.urlopen(base + path) as response:
            return response.status, response.headers.get_content_type(), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers.get_content_type(), exc.read()


def _get_json(base, path):
    status, content_type, body = _get(base, path)
    assert content_type == "application/json"
    return status, json.loads(body)


def test_index_is_inline_and_local(service):
    base, _, _ = service
    status, content_type, body = _get(base, "/")
    assert status == 200
    assert content_type == "text/html"
    text = body.decode("utf-8").lower()
    assert "lakeback" in text
    assert "cdn" not in text
    assert "https://" not in text


def test_tables_endpoint(service):
    base, _, _ = service
    status, payload = _get_json(base, "/api/tables")
    assert status == 200
    assert payload["table_count"] == 1
    table = payload["tables"][0]
    assert table["name"] == "orders"
    assert [item["version"] for item in table["snapshots"]] == [1, 2]


def test_travel_endpoint_with_pagination_and_partition(service):
    base, _, _ = service
    status, payload = _get_json(
        base, "/api/travel?table=orders&at=1&limit=2&offset=1&partition=dt=2024-02-01"
    )
    assert status == 200
    assert payload["snapshot"]["version"] == 1
    assert payload["returned"] <= 2
    assert payload["limit"] == 2
    assert payload["offset"] == 1
    assert all(row["dt"] == "2024-02-01" for row in payload["rows"])


def test_diff_endpoint(service):
    base, _, _ = service
    status, payload = _get_json(base, "/api/diff?table=orders&from=1&to=2")
    assert status == 200
    assert payload["counts"]["added"] >= 1
    assert [item["key"] for item in payload["modified"]] == sorted(
        item["key"] for item in payload["modified"]
    )


def test_report_endpoint(service):
    base, config, report = service
    status, payload = _get_json(
        base, f"/api/backfill/report?report_id={report['report_id']}"
    )
    assert status == 200
    assert payload["report_id"] == report["report_id"]
    assert payload["status"] == "residual_diff"
    assert load_report(config.reports_dir, report["report_id"])["status"] == "residual_diff"


@pytest.mark.parametrize(
    "path, status, code",
    [
        ("/api/travel?table=missing&at=1", 404, "not_found"),
        ("/api/travel?table=orders&at=99", 404, "not_found"),
        ("/api/travel?table=orders&at=banana", 400, "bad_request"),
        ("/api/travel?table=orders", 400, "bad_request"),
        ("/api/travel?table=orders&at=1&limit=abc", 400, "bad_request"),
        ("/api/backfill/report?report_id=../../etc/passwd", 400, "bad_request"),
        ("/api/backfill/report?report_id=rpt-0000000000000000", 404, "not_found"),
        ("/nope", 404, "not_found"),
    ],
)
def test_structured_error_responses(service, path, status, code):
    base, _, _ = service
    actual_status, payload = _get_json(base, path)
    assert actual_status == status
    assert payload["error"]["code"] == code
    assert payload["error"]["status"] == status


def test_write_methods_are_rejected(service):
    base, _, _ = service
    request = urllib.request.Request(base + "/api/tables", data=b"{}", method="POST")
    with pytest.raises(urllib.error.HTTPError) as caught:
        urllib.request.urlopen(request)
    assert caught.value.code == 405
    payload = json.loads(caught.value.read())
    assert payload["error"]["code"] == "method_not_allowed"


def test_non_loopback_binding_is_refused(make_config):
    config = make_config()
    with pytest.raises(ConfigError, match="non-loopback"):
        make_server(config, host=str(ipaddress.ip_address(1)), port=0)


def test_config_rejects_non_loopback_before_server(make_config):
    config = make_config()
    text = (config.path).read_text(encoding="utf-8").replace(
        "127.0.0.1", str(ipaddress.ip_address(1))
    )
    config.path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError):
        Config.load(config.path)
