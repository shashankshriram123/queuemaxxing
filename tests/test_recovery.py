from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.models import Message, MessageState, QueueConfig, QueueOrder
from app.queue_engine import (
    InvalidReceiptHandleError,
    QueueConfigurationMismatchError,
    QueueEngine,
)


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


def require_message(message: Message | None) -> Message:
    assert message is not None
    return message


def event_types(path: Path) -> list[str]:
    return [json.loads(line)["event_type"] for line in path.read_text().splitlines()]


def test_ready_message_survives_close_and_reopen(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    first = QueueEngine.open_durable(path)
    message = first.enqueue({"name": "ready"}, priority=7)
    first.close()

    second = QueueEngine.open_durable(path)
    try:
        recovered = second.get_message(message.id)
        assert recovered.payload == {"name": "ready"}
        assert recovered.priority == 7
        assert recovered.state is MessageState.READY
    finally:
        second.close()


def test_delayed_message_retains_original_availability_after_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    clock = ManualClock()
    first = QueueEngine.open_durable(path, clock=clock)
    message = first.enqueue({"name": "later"}, delay_seconds=30)
    first.close()

    clock.advance(10)
    second = QueueEngine.open_durable(path, clock=clock)
    try:
        recovered = second.get_message(message.id)
        assert recovered.available_at == message.available_at
        assert recovered.state is MessageState.DELAYED
        assert second.receive("worker") is None
    finally:
        second.close()


def test_message_becoming_eligible_while_stopped_recovers_ready(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    clock = ManualClock()
    first = QueueEngine.open_durable(path, clock=clock)
    message = first.enqueue({"name": "later"}, delay_seconds=5)
    first.close()

    clock.advance(5)
    second = QueueEngine.open_durable(path, clock=clock)
    try:
        assert second.get_message(message.id).state is MessageState.READY
        assert require_message(second.peek()).id == message.id
    finally:
        second.close()


def test_acked_message_remains_completed_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    clock = ManualClock()
    first = QueueEngine.open_durable(path, clock=clock)
    message = first.enqueue({"name": "done"})
    lease = require_message(first.receive("worker"))
    assert lease.receipt_handle is not None
    completed = first.ack(message.id, lease.receipt_handle)
    first.close()

    second = QueueEngine.open_durable(path, clock=clock)
    try:
        recovered = second.get_message(message.id)
        assert recovered.state is MessageState.COMPLETED
        assert recovered.completed_at == completed.completed_at
        assert recovered.receipt_handle is None
        assert second.receive("another-worker") is None
    finally:
        second.close()


def test_cleared_completed_messages_stay_removed_after_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    first = QueueEngine.open_durable(path)
    completed = first.enqueue({"name": "done"})
    retained = first.enqueue({"name": "ready"})
    lease = require_message(first.receive("worker"))
    assert lease.id == completed.id
    assert lease.receipt_handle is not None
    first.ack(completed.id, lease.receipt_handle)
    assert first.clear_completed() == 1
    first.close()

    second = QueueEngine.open_durable(path)
    try:
        assert [message.id for message in second.messages()] == [retained.id]
        next_message = second.enqueue({"name": "next"})
        assert next_message.sequence == 3
        assert "completed_messages_cleared" in event_types(path)
    finally:
        second.close()


def test_immediate_nack_recovers_ready(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    clock = ManualClock()
    first = QueueEngine.open_durable(path, clock=clock)
    message = first.enqueue({"name": "retry"})
    lease = require_message(first.receive("worker"))
    assert lease.receipt_handle is not None
    first.nack(message.id, lease.receipt_handle)
    first.close()

    second = QueueEngine.open_durable(path, clock=clock)
    try:
        recovered = second.get_message(message.id)
        assert recovered.state is MessageState.READY
        assert recovered.delivery_attempts == 1
    finally:
        second.close()


def test_delayed_nack_recovers_delayed_then_becomes_ready(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    clock = ManualClock()
    first = QueueEngine.open_durable(path, clock=clock)
    message = first.enqueue({"name": "retry-later"})
    lease = require_message(first.receive("worker"))
    assert lease.receipt_handle is not None
    retry = first.nack(message.id, lease.receipt_handle, retry_delay_seconds=20)
    first.close()

    clock.advance(10)
    second = QueueEngine.open_durable(path, clock=clock)
    assert second.get_message(message.id).state is MessageState.DELAYED
    assert second.get_message(message.id).available_at == retry.available_at
    second.close()

    clock.advance(10)
    third = QueueEngine.open_durable(path, clock=clock)
    try:
        assert third.get_message(message.id).state is MessageState.READY
    finally:
        third.close()


def test_unexpired_lease_and_receipt_ownership_survive_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    clock = ManualClock()
    first = QueueEngine.open_durable(path, clock=clock)
    message = first.enqueue({"name": "leased"})
    lease = require_message(
        first.receive("worker-9", visibility_timeout_seconds=30)
    )
    first.close()

    clock.advance(10)
    second = QueueEngine.open_durable(path, clock=clock)
    try:
        recovered = second.get_message(message.id)
        assert recovered.state is MessageState.IN_FLIGHT
        assert recovered.leased_by == "worker-9"
        assert recovered.receipt_handle == lease.receipt_handle
        assert recovered.lease_expires_at == lease.lease_expires_at
        assert recovered.delivery_attempts == 1
        assert second.receive("worker-10") is None
    finally:
        second.close()


def test_expired_lease_recovers_ready_and_records_expiration_once(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    clock = ManualClock()
    first = QueueEngine.open_durable(path, clock=clock)
    message = first.enqueue({"name": "expired"})
    first.receive("worker", visibility_timeout_seconds=5)
    first.close()
    before_recovery = len(event_types(path))

    clock.advance(5)
    second = QueueEngine.open_durable(path, clock=clock)
    assert second.get_message(message.id).state is MessageState.READY
    assert second.get_message(message.id).delivery_attempts == 1
    second.close()

    after_first_recovery = len(event_types(path))
    third = QueueEngine.open_durable(path, clock=clock)
    third.close()

    assert after_first_recovery == before_recovery + 1
    assert event_types(path)[-1] == "lease_expired"
    assert len(event_types(path)) == after_first_recovery


def test_redelivery_attempts_and_stale_receipts_survive_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    clock = ManualClock()
    first = QueueEngine.open_durable(path, clock=clock)
    message = first.enqueue({"name": "redeliver"})
    first_lease = require_message(
        first.receive("worker-1", visibility_timeout_seconds=4)
    )
    assert first_lease.receipt_handle is not None
    first.close()

    clock.advance(4)
    second = QueueEngine.open_durable(path, clock=clock)
    second_lease = require_message(second.receive("worker-2"))
    assert second_lease.receipt_handle is not None
    assert second_lease.receipt_handle != first_lease.receipt_handle
    assert second_lease.delivery_attempts == 2
    second.close()

    third = QueueEngine.open_durable(path, clock=clock)
    try:
        with pytest.raises(InvalidReceiptHandleError):
            third.ack(message.id, first_lease.receipt_handle)
        recovered = third.get_message(message.id)
        assert recovered.delivery_attempts == 2
        assert recovered.receipt_handle == second_lease.receipt_handle
    finally:
        third.close()


@pytest.mark.parametrize(
    "config",
    [
        QueueConfig(order=QueueOrder.FIFO, priority_enabled=False),
        QueueConfig(order=QueueOrder.LIFO, priority_enabled=True),
    ],
)
def test_queue_configuration_and_ordering_survive_restart(
    tmp_path: Path, config: QueueConfig
) -> None:
    path = tmp_path / f"{config.order.value}.wal"
    first = QueueEngine.open_durable(path, config)
    first.enqueue({"name": "old-low"}, priority=1)
    first.enqueue({"name": "new-high"}, priority=10)
    first.close()

    second = QueueEngine.open_durable(path)
    try:
        assert second.config == config
        names = [message.payload["name"] for message in second.ready_messages()]
        expected = (
            ["new-high", "old-low"]
            if config.priority_enabled or config.order is QueueOrder.LIFO
            else ["old-low", "new-high"]
        )
        assert names == expected
    finally:
        second.close()


def test_conflicting_queue_configuration_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    first = QueueEngine.open_durable(
        path, QueueConfig(order=QueueOrder.LIFO, priority_enabled=True)
    )
    first.close()

    with pytest.raises(QueueConfigurationMismatchError):
        QueueEngine.open_durable(
            path, QueueConfig(order=QueueOrder.FIFO, priority_enabled=False)
        )


def test_sequence_numbers_continue_without_reuse_after_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    first = QueueEngine.open_durable(path)
    original = [first.enqueue({"index": index}) for index in range(4)]
    first.close()

    second = QueueEngine.open_durable(path)
    try:
        later = [second.enqueue({"index": index}) for index in range(4, 8)]
        all_messages = second.messages()
        assert [message.sequence for message in original + later] == list(range(1, 9))
        assert [message.sequence for message in all_messages] == list(range(1, 9))
        assert len({message.id for message in all_messages}) == 8
    finally:
        second.close()


def test_fresh_recovered_engine_matches_prior_durable_state(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    clock = ManualClock()
    first = QueueEngine.open_durable(
        path, QueueConfig(order=QueueOrder.LIFO, priority_enabled=True), clock
    )
    first.enqueue({"name": "ready"}, priority=2)
    first.enqueue({"name": "delayed"}, priority=9, delay_seconds=50)
    first.enqueue({"name": "leased"}, priority=20)
    leased = require_message(first.receive("worker", visibility_timeout_seconds=40))
    assert leased.payload == {"name": "leased"}
    before = first.messages()
    first.close()

    second = QueueEngine.open_durable(path, clock=clock)
    try:
        assert second.messages() == before
    finally:
        second.close()


def test_replay_and_repeated_open_close_do_not_duplicate_events(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    first = QueueEngine.open_durable(path)
    first.enqueue({"name": "one"})
    first.close()
    original_bytes = path.read_bytes()

    for _ in range(5):
        reopened = QueueEngine.open_durable(path)
        reopened.close()

    assert path.read_bytes() == original_bytes


def test_concurrent_durable_producers_have_valid_history_and_no_loss(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    producer_count = 6
    per_producer = 40
    barrier = threading.Barrier(producer_count)
    engine = QueueEngine.open_durable(path)

    def produce(producer_id: int) -> list[str]:
        barrier.wait()
        return [
            engine.enqueue({"producer": producer_id, "index": index}).id
            for index in range(per_producer)
        ]

    with ThreadPoolExecutor(max_workers=producer_count) as pool:
        batches = list(pool.map(produce, range(producer_count)))
    created_ids = {message_id for batch in batches for message_id in batch}
    engine.close()

    recovered = QueueEngine.open_durable(path)
    try:
        messages = recovered.messages()
        records = [json.loads(line) for line in path.read_text().splitlines()]
        assert len(messages) == producer_count * per_producer
        assert {message.id for message in messages} == created_ids
        assert [record["record_number"] for record in records] == list(
            range(1, len(records) + 1)
        )
    finally:
        recovered.close()


def test_concurrent_durable_consumers_create_no_conflicting_claims(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    message_count = 120
    consumer_count = 10
    barrier = threading.Barrier(consumer_count)
    engine = QueueEngine.open_durable(path)
    for index in range(message_count):
        engine.enqueue({"index": index})

    def consume(worker: int) -> list[str]:
        barrier.wait()
        leased: list[str] = []
        while True:
            message = engine.receive(f"worker-{worker}")
            if message is None:
                return leased
            leased.append(message.id)

    with ThreadPoolExecutor(max_workers=consumer_count) as pool:
        batches = list(pool.map(consume, range(consumer_count)))
    leased_ids = [message_id for batch in batches for message_id in batch]
    engine.close()

    records = [json.loads(line) for line in path.read_text().splitlines()]
    claims = [record for record in records if record["event_type"] == "message_claimed"]
    recovered = QueueEngine.open_durable(path)
    try:
        assert len(leased_ids) == message_count
        assert len(set(leased_ids)) == message_count
        assert len(claims) == message_count
        assert len({record["data"]["message_id"] for record in claims}) == message_count
        assert all(
            message.state is MessageState.IN_FLIGHT
            for message in recovered.messages()
        )
    finally:
        recovered.close()
