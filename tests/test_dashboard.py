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
    assert "Durable queue lab" in response.text
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
        "mode-single",
        "mode-burst",
        "send-one",
        "burst-size",
        "send-burst",
        "burst-status",
        "consumer-form",
        "worker-count",
        "visibility-timeout",
        "processing-time",
        "worker-prompt",
        "worker-prompt-title",
        "worker-prompt-copy",
        "start-workers",
        "stop-workers",
        "worker-activity",
        "current-lease",
        "queue-settings",
        "scenario-select",
        "scenario-description",
        "run-scenario",
        "connection-banner",
        "stat-total",
        "stat-active-workers",
        "lane-delayed",
        "lane-ready",
        "lane-in-flight",
        "lane-completed",
        "clear-completed",
        "event-panel",
        "event-list",
    }

    for element_id in expected_ids:
        assert f'id="{element_id}"' in html

    assert (
        'id="visibility-timeout" type="number" min="0.1" step="0.1"'
        in html
    )
    assert '<option value="standard">' in html
    assert '<option value="priority">' in html
    assert '<option value="delayed">' in html
    assert '<option value="flash">' in html
    assert '<option value="vip">' in html
    assert '<option value="backfill">' in html
    assert '<option value="failure">' in html
    assert 'data-workflow-step="1"' in html
    assert 'data-workflow-step="4"' in html
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
    assert ".scenario-launcher" in stylesheet.text
    assert ".workflow-steps" in stylesheet.text
    assert ".status-strip" in stylesheet.text
    assert ".worker-activity" in stylesheet.text
    assert ".run-pane.has-ready-work" in stylesheet.text
    assert ".message-card.tone-4" in stylesheet.text
    assert "worker-callout" in stylesheet.text

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
    assert "setComposerMode" in script.text
    assert "scenarioCopy" in script.text
    assert "setWorkflowStep" in script.text
    assert "updateWorkerGuidance" in script.text
    assert "tone-${toneIndex}" in script.text
    assert 'api("/api/messages/completed", { method: "DELETE" })' in script.text
    assert "Confirm clearing" in script.text
    assert "textContent" in script.text
    assert "innerHTML" not in script.text


def test_fastapi_documentation_remains_available(
    dashboard_client: TestClient,
) -> None:
    response = dashboard_client.get("/docs")

    assert response.status_code == 200
    assert "Swagger UI" in response.text
