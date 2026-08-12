from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.models import QueueConfig, QueueOrder
from app.queue_engine import QueueEngine
from app.wal import WALWriteError, WriteAheadLog


@dataclass
class ManualClock:
    current: datetime = field(
        default_factory=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __call__(self) -> datetime:
        with self._lock:
            return self.current

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.current += timedelta(seconds=seconds)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def wal_path(tmp_path: Path) -> Path:
    return tmp_path / "queue.wal"


@pytest.fixture
def client(wal_path: Path, clock: ManualClock) -> Iterator[TestClient]:
    with TestClient(create_app(wal_path=wal_path, clock=clock)) as test_client:
        yield test_client


def enqueue(
    client: TestClient,
    name: str,
    *,
    priority: int = 0,
    delay_seconds: float = 0,
) -> dict:
    response = client.post(
        "/api/messages",
        json={
            "payload": {"name": name},
            "priority": priority,
            "delay_seconds": delay_seconds,
        },
    )
    assert response.status_code == 201
    return response.json()


def receive(
    client: TestClient,
    worker_id: str = "worker-1",
    visibility_timeout_seconds: float = 30,
) -> dict | None:
    response = client.post(
        "/api/messages/receive",
        json={
            "worker_id": worker_id,
            "visibility_timeout_seconds": visibility_timeout_seconds,
        },
    )
    assert response.status_code == 200
    return response.json()["message"]


def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "queuemaxxing"}


def test_read_default_configuration(client: TestClient) -> None:
    response = client.get("/api/config")

    assert response.status_code == 200
    assert response.json() == {"order": "fifo", "priority_enabled": False}


def test_update_fifo_lifo_configuration_changes_ready_order(client: TestClient) -> None:
    first = enqueue(client, "first")
    second = enqueue(client, "second")

    response = client.put(
        "/api/config", json={"order": "lifo", "priority_enabled": False}
    )

    assert response.status_code == 200
    assert response.json() == {"order": "lifo", "priority_enabled": False}
    leased = receive(client)
    assert leased is not None
    assert leased["id"] == second["id"]
    assert leased["id"] != first["id"]


def test_update_priority_configuration_changes_ready_order(client: TestClient) -> None:
    enqueue(client, "old-low", priority=1)
    high = enqueue(client, "new-high", priority=10)

    response = client.put(
        "/api/config", json={"order": "fifo", "priority_enabled": True}
    )

    assert response.status_code == 200
    leased = receive(client)
    assert leased is not None
    assert leased["id"] == high["id"]


def test_configuration_update_survives_application_restart(
    wal_path: Path, clock: ManualClock
) -> None:
    with TestClient(create_app(wal_path=wal_path, clock=clock)) as first:
        response = first.put(
            "/api/config", json={"order": "lifo", "priority_enabled": True}
        )
        assert response.status_code == 200

    with TestClient(create_app(wal_path=wal_path, clock=clock)) as second:
        assert second.get("/api/config").json() == {
            "order": "lifo",
            "priority_enabled": True,
        }


def test_enqueue_returns_created_public_message(client: TestClient) -> None:
    response = client.post(
        "/api/messages",
        json={
            "payload": {"order_id": 104, "nested": {"status": "new"}},
            "priority": 8,
            "delay_seconds": 3,
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["payload"]["order_id"] == 104
    assert body["priority"] == 8
    assert body["state"] == "delayed"
    assert body["sequence"] == 1
    assert "receipt_handle" not in body


@pytest.mark.parametrize(
    ("body", "content"),
    [
        ({"payload": [], "priority": 0, "delay_seconds": 0}, None),
        ({"payload": {}, "priority": 1.5, "delay_seconds": 0}, None),
        ({"payload": {}, "priority": 1, "delay_seconds": -1}, None),
        (None, b'{"payload":'),
    ],
)
def test_enqueue_validation_errors(
    client: TestClient, body: dict | None, content: bytes | None
) -> None:
    if content is not None:
        response = client.post(
            "/api/messages", content=content, headers={"content-type": "application/json"}
        )
    else:
        response = client.post("/api/messages", json=body)

    assert response.status_code == 422


def test_list_messages_and_filter_by_state(client: TestClient) -> None:
    ready = enqueue(client, "ready")
    delayed = enqueue(client, "delayed", delay_seconds=50)
    lease = receive(client)
    assert lease is not None
    assert lease["id"] == ready["id"]

    all_messages = client.get("/api/messages")
    delayed_messages = client.get("/api/messages", params={"state": "delayed"})
    in_flight_messages = client.get(
        "/api/messages", params={"state": "in_flight"}
    )

    assert all_messages.status_code == 200
    assert {message["id"] for message in all_messages.json()} == {
        ready["id"],
        delayed["id"],
    }
    assert [message["id"] for message in delayed_messages.json()] == [delayed["id"]]
    assert [message["id"] for message in in_flight_messages.json()] == [ready["id"]]
    assert all("receipt_handle" not in message for message in all_messages.json())
    assert client.get("/api/messages", params={"state": "invalid"}).status_code == 422


def test_receive_returns_lease_and_empty_queue_returns_null(client: TestClient) -> None:
    assert receive(client) is None
    message = enqueue(client, "job")

    leased = receive(client, "worker-7", 12)

    assert leased is not None
    assert leased["id"] == message["id"]
    assert leased["state"] == "in_flight"
    assert leased["leased_by"] == "worker-7"
    assert leased["delivery_attempts"] == 1
    assert leased["receipt_handle"]
    assert leased["lease_expires_at"]


def test_receive_validation_errors(client: TestClient) -> None:
    enqueue(client, "job")
    for body in (
        {"worker_id": " ", "visibility_timeout_seconds": 10},
        {"worker_id": "worker", "visibility_timeout_seconds": 0},
        {"worker_id": "worker", "visibility_timeout_seconds": -1},
    ):
        assert client.post("/api/messages/receive", json=body).status_code == 422


def test_ack_with_valid_receipt_completes_message(client: TestClient) -> None:
    message = enqueue(client, "job")
    lease = receive(client)
    assert lease is not None

    response = client.post(
        f"/api/messages/{message['id']}/ack",
        json={"receipt_handle": lease["receipt_handle"]},
    )

    assert response.status_code == 200
    assert response.json()["state"] == "completed"
    assert "receipt_handle" not in response.json()


def test_nack_supports_immediate_and_delayed_retry(client: TestClient) -> None:
    immediate = enqueue(client, "immediate")
    immediate_lease = receive(client)
    assert immediate_lease is not None
    response = client.post(
        f"/api/messages/{immediate['id']}/nack",
        json={"receipt_handle": immediate_lease["receipt_handle"]},
    )
    assert response.status_code == 200
    assert response.json()["state"] == "ready"
    retried = receive(client, "worker-retry")
    assert retried is not None
    assert retried["id"] == immediate["id"]
    assert client.post(
        f"/api/messages/{immediate['id']}/ack",
        json={"receipt_handle": retried["receipt_handle"]},
    ).status_code == 200

    delayed = enqueue(client, "delayed", priority=100)
    delayed_lease = receive(client, "worker-2")
    assert delayed_lease is not None
    response = client.post(
        f"/api/messages/{delayed['id']}/nack",
        json={
            "receipt_handle": delayed_lease["receipt_handle"],
            "retry_delay_seconds": 25,
        },
    )
    assert response.status_code == 200
    assert response.json()["state"] == "delayed"


def test_negative_nack_retry_delay_is_rejected(client: TestClient) -> None:
    message = enqueue(client, "job")
    lease = receive(client)
    assert lease is not None
    response = client.post(
        f"/api/messages/{message['id']}/nack",
        json={
            "receipt_handle": lease["receipt_handle"],
            "retry_delay_seconds": -1,
        },
    )
    assert response.status_code == 422


def test_unknown_message_errors_are_404(client: TestClient) -> None:
    for action in ("ack", "nack"):
        response = client.post(
            f"/api/messages/missing/{action}",
            json={"receipt_handle": "unknown-receipt"},
        )
        assert response.status_code == 404


def test_stale_receipt_and_invalid_state_transitions_are_409(
    client: TestClient, clock: ManualClock
) -> None:
    ready = enqueue(client, "ready")
    invalid_state = client.post(
        f"/api/messages/{ready['id']}/ack",
        json={"receipt_handle": "not-leased"},
    )
    assert invalid_state.status_code == 409

    first_lease = receive(client, visibility_timeout_seconds=5)
    assert first_lease is not None
    clock.advance(5)
    second_lease = receive(client, worker_id="worker-2")
    assert second_lease is not None
    stale = client.post(
        f"/api/messages/{ready['id']}/ack",
        json={"receipt_handle": first_lease["receipt_handle"]},
    )
    assert stale.status_code == 409
    assert "active lease" in stale.json()["detail"]


def test_stats_reflect_real_activity(
    client: TestClient, clock: ManualClock
) -> None:
    completed = enqueue(client, "completed")
    first = receive(client, "worker-a")
    assert first is not None
    client.post(
        f"/api/messages/{completed['id']}/ack",
        json={"receipt_handle": first["receipt_handle"]},
    )

    enqueue(client, "redelivered")
    old_lease = receive(client, "worker-b", visibility_timeout_seconds=4)
    assert old_lease is not None
    clock.advance(4)
    assert receive(client, "worker-c") is not None

    response = client.get("/api/stats")

    assert response.status_code == 200
    assert response.json() == {
        "total": 2,
        "delayed": 0,
        "ready": 0,
        "in_flight": 1,
        "completed": 1,
        "total_delivery_attempts": 3,
        "redelivery_count": 1,
        "active_worker_count": 1,
    }


def test_clear_completed_endpoint_removes_only_completed_messages(
    client: TestClient,
) -> None:
    completed = enqueue(client, "completed")
    retained = enqueue(client, "ready")
    lease = receive(client)
    assert lease is not None
    assert lease["id"] == completed["id"]
    ack = client.post(
        f"/api/messages/{completed['id']}/ack",
        json={"receipt_handle": lease["receipt_handle"]},
    )
    assert ack.status_code == 200

    response = client.delete("/api/messages/completed")

    assert response.status_code == 200
    assert response.json() == {"cleared": 1}
    assert [message["id"] for message in client.get("/api/messages").json()] == [
        retained["id"]
    ]
    assert client.get("/api/stats").json()["completed"] == 0
    assert client.delete("/api/messages/completed").json() == {"cleared": 0}


def test_events_reflect_activity_and_never_expose_receipts(client: TestClient) -> None:
    message = enqueue(client, "job")
    lease = receive(client)
    assert lease is not None
    client.post(
        f"/api/messages/{message['id']}/ack",
        json={"receipt_handle": lease["receipt_handle"]},
    )

    response = client.get("/api/events", params={"limit": 3})

    assert response.status_code == 200
    body = response.json()
    assert body["ordering"] == "oldest_first"
    assert [event["event_type"] for event in body["events"]] == [
        "message_enqueued",
        "message_claimed",
        "message_acked",
    ]
    assert all(event["message_id"] == message["id"] for event in body["events"])
    assert "receipt_handle" not in response.text
    assert client.get("/api/events", params={"limit": 0}).status_code == 422
    assert client.get("/api/events", params={"limit": 201}).status_code == 422


def test_api_created_message_survives_fresh_application(
    wal_path: Path, clock: ManualClock
) -> None:
    with TestClient(create_app(wal_path=wal_path, clock=clock)) as first:
        created = enqueue(first, "durable", priority=4)

    with TestClient(create_app(wal_path=wal_path, clock=clock)) as second:
        messages = second.get("/api/messages").json()
        assert len(messages) == 1
        assert messages[0]["id"] == created["id"]
        assert messages[0]["payload"] == {"name": "durable"}


def test_application_shutdown_releases_wal_lock(
    wal_path: Path, clock: ManualClock
) -> None:
    with TestClient(create_app(wal_path=wal_path, clock=clock)) as first:
        assert first.get("/api/config").status_code == 200

    recovered = QueueEngine.open_durable(wal_path, clock=clock)
    recovered.close()


def test_storage_failure_maps_to_sanitized_503(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wal = WriteAheadLog(tmp_path / "queue.wal")
    engine = QueueEngine(wal=wal)

    def fail_append(*_: object, **__: object) -> None:
        raise WALWriteError("/sensitive/internal/path failed")

    monkeypatch.setattr(wal, "append", fail_append)
    with TestClient(create_app(engine=engine)) as client:
        response = client.post(
            "/api/messages",
            json={"payload": {"name": "job"}, "priority": 0, "delay_seconds": 0},
        )

    assert response.status_code == 503
    assert response.json() == {"detail": "queue storage is temporarily unavailable"}
    assert "sensitive" not in response.text
    engine.close()


def test_configuration_request_validation(client: TestClient) -> None:
    assert client.put(
        "/api/config", json={"order": "random", "priority_enabled": True}
    ).status_code == 422
    assert client.put(
        "/api/config", json={"order": "fifo", "priority_enabled": 1}
    ).status_code == 422
