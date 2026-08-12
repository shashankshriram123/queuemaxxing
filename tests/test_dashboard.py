from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def dashboard_client(tmp_path: Path):
    with TestClient(create_app(wal_path=tmp_path / "dashboard.wal")) as client:
        yield client


def test_root_serves_dashboard(dashboard_client: TestClient) -> None:
    response = dashboard_client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Artie QueueLab" in response.text
    assert "Durable configurable queue" in response.text
    assert 'id="server-status"' in response.text


def test_dashboard_contains_real_queue_controls(dashboard_client: TestClient) -> None:
    html = dashboard_client.get("/").text

    expected_ids = {
        "config-form",
        "queue-order",
        "priority-enabled",
        "enqueue-form",
        "payload-editor",
        "message-priority",
        "message-delay",
        "consumer-form",
        "worker-id",
        "visibility-timeout",
        "current-lease",
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


def test_dashboard_static_assets_are_served(dashboard_client: TestClient) -> None:
    stylesheet = dashboard_client.get("/static/styles.css")
    script = dashboard_client.get("/static/app.js")

    assert stylesheet.status_code == 200
    assert "text/css" in stylesheet.headers["content-type"]
    assert "@media (max-width:" in stylesheet.text
    assert "prefers-reduced-motion" in stylesheet.text
    assert ":focus-visible" in stylesheet.text

    assert script.status_code == 200
    assert "javascript" in script.headers["content-type"]
    assert 'api("/api/messages")' in script.text
    assert 'api("/api/stats")' in script.text
    assert 'api("/api/events?limit=40")' in script.text
    assert "browserReceipts" in script.text
    assert "pollInFlight" in script.text
    assert "textContent" in script.text
    assert "innerHTML" not in script.text


def test_fastapi_documentation_remains_available(
    dashboard_client: TestClient,
) -> None:
    response = dashboard_client.get("/docs")

    assert response.status_code == 200
    assert "Swagger UI" in response.text
