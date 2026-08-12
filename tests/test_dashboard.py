from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def dashboard_client(tmp_path: Path) -> Iterator[TestClient]:
    with TestClient(create_app(wal_path=tmp_path / "dashboard.wal")) as client:
        yield client


def test_root_serves_dashboard(dashboard_client: TestClient) -> None:
    response = dashboard_client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "QueueMaxxing" in response.text
    assert "Durable queue operations console" in response.text
    assert "Independent engineering demo · Not affiliated with Artie" in response.text
    assert "Artie QueueLab" not in response.text
    assert 'id="server-status"' in response.text


def test_dashboard_contains_real_queue_controls(dashboard_client: TestClient) -> None:
    html = dashboard_client.get("/").text

    expected_ids = {
        "config-form",
        "config-badge",
        "queue-order",
        "priority-enabled",
        "enqueue-form",
        "payload-editor",
        "message-priority",
        "message-delay",
        "burst-size",
        "send-burst",
        "burst-status",
        "consumer-form",
        "worker-count",
        "visibility-timeout",
        "processing-time",
        "start-workers",
        "stop-workers",
        "worker-activity",
        "current-lease",
        "connection-banner",
        "stat-total",
        "stat-active-workers",
        "lane-delayed",
        "lane-ready",
        "lane-in-flight",
        "lane-completed",
        "event-panel",
        "event-list",
    }

    for element_id in expected_ids:
        assert f'id="{element_id}"' in html

    assert (
        'id="visibility-timeout" type="number" min="0.1" step="0.1"'
        in html
    )
    assert "data-example=\"standard\"" in html
    assert "data-example=\"priority\"" in html
    assert "data-example=\"delayed\"" in html
    assert "data-scenario=\"flash\"" in html
    assert "data-scenario=\"vip\"" in html
    assert "data-scenario=\"backfill\"" in html
    assert "data-scenario=\"failure\"" in html
    assert 'href="static/styles.css"' in html
    assert 'src="static/app.js"' in html
    assert '<details id="event-panel"' in html
    assert '<details id="event-panel" class="event-drawer" open' not in html


def test_dashboard_static_assets_are_served(dashboard_client: TestClient) -> None:
    stylesheet = dashboard_client.get("/static/styles.css")
    script = dashboard_client.get("/static/app.js")

    assert stylesheet.status_code == 200
    assert "text/css" in stylesheet.headers["content-type"]
    assert "@media (max-width:" in stylesheet.text
    assert "prefers-reduced-motion" in stylesheet.text
    assert ":focus-visible" in stylesheet.text
    assert "overflow: auto" in stylesheet.text
    assert "@media (max-width: 720px)" in stylesheet.text
    assert ".scenario-strip" in stylesheet.text
    assert ".worker-activity" in stylesheet.text

    assert script.status_code == 200
    assert "javascript" in script.headers["content-type"]
    assert 'api("/api/messages")' in script.text
    assert 'api("/api/stats")' in script.text
    assert 'api("/api/events?limit=40")' in script.text
    assert "browserReceipts" in script.text
    assert "pollInFlight" in script.text
    assert "Promise.allSettled(requests)" in script.text
    assert "simulatedWorkerLoop" in script.text
    assert "visibility_timeout_seconds" in script.text
    assert 'worker_id: "simulated-failure"' in script.text
    assert "showDirectFileNotice" in script.text
    assert 'window.location.protocol === "file:"' in script.text
    assert 'setConnection("offline")' in script.text
    assert "error.status === 409" in script.text
    assert "stale or its lease expired" in script.text
    assert "data-example" in script.text
    assert "textContent" in script.text
    assert "innerHTML" not in script.text


def test_fastapi_documentation_remains_available(
    dashboard_client: TestClient,
) -> None:
    response = dashboard_client.get("/docs")

    assert response.status_code == 200
    assert "Swagger UI" in response.text
