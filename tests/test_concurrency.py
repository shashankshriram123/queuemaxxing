from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pytest

from app.models import Message, MessageState, QueueConfig, QueueOrder
from app.queue_engine import (
    InvalidReceiptHandleError,
    MessageNotInFlightError,
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


def test_concurrent_producers_preserve_every_message_and_sequence() -> None:
    producer_count = 8
    messages_per_producer = 100
    barrier = threading.Barrier(producer_count)
    engine = QueueEngine(QueueConfig(), ManualClock())

    def produce(producer_id: int) -> list[tuple[str, int]]:
        barrier.wait()
        created = []
        for index in range(messages_per_producer):
            message = engine.enqueue({"producer": producer_id, "index": index})
            created.append((message.id, message.sequence))
        return created

    with ThreadPoolExecutor(max_workers=producer_count) as pool:
        batches = list(pool.map(produce, range(producer_count)))

    returned = [item for batch in batches for item in batch]
    stored = engine.messages()
    expected_count = producer_count * messages_per_producer

    assert len(returned) == expected_count
    assert len(stored) == expected_count
    assert len({message_id for message_id, _ in returned}) == expected_count
    assert len({sequence for _, sequence in returned}) == expected_count
    assert {message.id for message in stored} == {
        message_id for message_id, _ in returned
    }
    assert {message.sequence for message in stored} == set(
        range(1, expected_count + 1)
    )


def test_concurrent_consumers_never_share_an_active_lease() -> None:
    message_count = 240
    consumer_count = 16
    barrier = threading.Barrier(consumer_count)
    engine = QueueEngine(QueueConfig(), ManualClock())
    for index in range(message_count):
        engine.enqueue({"index": index})

    def consume(worker_index: int) -> list[tuple[str, str]]:
        barrier.wait()
        leased: list[tuple[str, str]] = []
        while True:
            message = engine.receive(f"worker-{worker_index}")
            if message is None:
                return leased
            assert message.receipt_handle is not None
            leased.append((message.id, message.receipt_handle))

    with ThreadPoolExecutor(max_workers=consumer_count) as pool:
        batches = list(pool.map(consume, range(consumer_count)))

    leases = [lease for batch in batches for lease in batch]

    assert len(leases) == message_count
    assert len({message_id for message_id, _ in leases}) == message_count
    assert len({receipt for _, receipt in leases}) == message_count
    assert all(
        message.state is MessageState.IN_FLIGHT for message in engine.messages()
    )


def test_only_one_concurrent_receiver_gets_a_single_message() -> None:
    contender_count = 20
    barrier = threading.Barrier(contender_count)
    engine = QueueEngine(QueueConfig(), ManualClock())
    expected = engine.enqueue({"name": "single"})

    def contend(index: int) -> Message | None:
        barrier.wait()
        return engine.receive(f"worker-{index}")

    with ThreadPoolExecutor(max_workers=contender_count) as pool:
        results = list(pool.map(contend, range(contender_count)))

    winners = [message for message in results if message is not None]
    assert len(winners) == 1
    assert winners[0].id == expected.id
    assert sum(message is None for message in results) == contender_count - 1


def test_concurrent_acks_complete_different_messages() -> None:
    message_count = 120
    barrier = threading.Barrier(message_count)
    engine = QueueEngine(QueueConfig(), ManualClock())
    leases = []
    for index in range(message_count):
        engine.enqueue({"index": index})
        leases.append(require_message(engine.receive(f"worker-{index}")))

    def acknowledge(message: Message) -> str:
        assert message.receipt_handle is not None
        barrier.wait()
        return engine.ack(message.id, message.receipt_handle).id

    with ThreadPoolExecutor(max_workers=message_count) as pool:
        completed_ids = list(pool.map(acknowledge, leases))

    snapshots = engine.messages()
    assert len(set(completed_ids)) == message_count
    assert len(snapshots) == message_count
    assert all(message.state is MessageState.COMPLETED for message in snapshots)
    assert all(message.receipt_handle is None for message in snapshots)


def test_exactly_one_concurrent_ack_succeeds_for_one_lease() -> None:
    contender_count = 12
    barrier = threading.Barrier(contender_count)
    engine = QueueEngine(QueueConfig(), ManualClock())
    message = engine.enqueue({"name": "one-lease"})
    lease = require_message(engine.receive("worker"))
    assert lease.receipt_handle is not None

    def acknowledge(_: int) -> str:
        barrier.wait()
        try:
            engine.ack(message.id, lease.receipt_handle or "")
            return "completed"
        except MessageNotInFlightError:
            return "rejected"

    with ThreadPoolExecutor(max_workers=contender_count) as pool:
        outcomes = list(pool.map(acknowledge, range(contender_count)))

    assert outcomes.count("completed") == 1
    assert outcomes.count("rejected") == contender_count - 1
    assert engine.get_message(message.id).state is MessageState.COMPLETED


def test_ack_and_nack_race_allows_exactly_one_transition() -> None:
    barrier = threading.Barrier(3)
    engine = QueueEngine(QueueConfig(), ManualClock())
    message = engine.enqueue({"name": "contested"})
    lease = require_message(engine.receive("worker"))
    assert lease.receipt_handle is not None

    def transition(operation: str) -> tuple[str, str]:
        barrier.wait()
        try:
            if operation == "ack":
                result = engine.ack(message.id, lease.receipt_handle or "")
            else:
                result = engine.nack(message.id, lease.receipt_handle or "")
            return "succeeded", result.state.value
        except MessageNotInFlightError:
            return "rejected", operation

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(transition, operation) for operation in ("ack", "nack")]
        barrier.wait()
        outcomes = [future.result() for future in futures]

    assert [status for status, _ in outcomes].count("succeeded") == 1
    assert [status for status, _ in outcomes].count("rejected") == 1
    assert engine.get_message(message.id).state in {
        MessageState.COMPLETED,
        MessageState.READY,
    }


def test_concurrent_enqueue_and_receive_accounts_for_every_message() -> None:
    producer_count = 4
    consumer_count = 4
    messages_per_producer = 150
    receives_per_consumer = 150
    barrier = threading.Barrier(producer_count + consumer_count)
    engine = QueueEngine(QueueConfig(), ManualClock())

    def produce(producer_id: int) -> list[str]:
        barrier.wait()
        return [
            engine.enqueue({"producer": producer_id, "index": index}).id
            for index in range(messages_per_producer)
        ]

    def consume(consumer_id: int) -> list[str]:
        barrier.wait()
        completed: list[str] = []
        for _ in range(receives_per_consumer):
            message = engine.receive(f"consumer-{consumer_id}")
            if message is None:
                continue
            assert message.receipt_handle is not None
            engine.ack(message.id, message.receipt_handle)
            completed.append(message.id)
        return completed

    with ThreadPoolExecutor(max_workers=producer_count + consumer_count) as pool:
        producer_futures = [pool.submit(produce, index) for index in range(producer_count)]
        consumer_futures = [pool.submit(consume, index) for index in range(consumer_count)]
        created = [
            message_id
            for future in producer_futures
            for message_id in future.result()
        ]
        completed = [
            message_id
            for future in consumer_futures
            for message_id in future.result()
        ]

    snapshots = engine.messages()
    expected_count = producer_count * messages_per_producer
    accounted_states = {
        MessageState.READY,
        MessageState.DELAYED,
        MessageState.IN_FLIGHT,
        MessageState.COMPLETED,
    }

    assert len(created) == expected_count
    assert len(set(created)) == expected_count
    assert len(snapshots) == expected_count
    assert len({message.id for message in snapshots}) == expected_count
    assert len({message.sequence for message in snapshots}) == expected_count
    assert {message.id for message in snapshots} == set(created)
    assert set(completed).issubset(set(created))
    assert all(message.state in accounted_states for message in snapshots)


def test_public_results_are_deep_defensive_copies() -> None:
    original_payload = {"name": "safe", "nested": {"values": [1, 2]}}
    engine = QueueEngine(QueueConfig(), ManualClock())
    created = engine.enqueue(original_payload)

    original_payload["nested"]["values"].append(3)
    created.payload["name"] = "changed"
    created.payload["nested"]["values"].append(4)
    inspected = engine.get_message(created.id)
    inspected.state = MessageState.COMPLETED
    inspected.payload["nested"]["values"].append(5)
    listed = engine.messages()
    listed[0].payload["nested"]["values"].append(6)
    ready = engine.ready_messages()
    ready[0].payload["nested"]["values"].append(7)

    stored = engine.get_message(created.id)
    assert stored.state is MessageState.READY
    assert stored.payload == {"name": "safe", "nested": {"values": [1, 2]}}

    lease = require_message(engine.receive("worker"))
    lease.payload["name"] = "changed after receive"
    lease.state = MessageState.COMPLETED

    stored_lease = engine.get_message(created.id)
    assert stored_lease.payload["name"] == "safe"
    assert stored_lease.state is MessageState.IN_FLIGHT


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        (QueueConfig(order=QueueOrder.FIFO), ["old", "new"]),
        (QueueConfig(order=QueueOrder.LIFO), ["new", "old"]),
        (
            QueueConfig(order=QueueOrder.FIFO, priority_enabled=True),
            ["new", "old"],
        ),
    ],
)
def test_ordering_regression(config: QueueConfig, expected: list[str]) -> None:
    engine = QueueEngine(config, ManualClock())
    engine.enqueue({"name": "old"}, priority=1)
    engine.enqueue({"name": "new"}, priority=10)

    assert [message.payload["name"] for message in engine.ready_messages()] == expected


def test_delay_promotion_and_expired_lease_redelivery_regression() -> None:
    clock = ManualClock()
    engine = QueueEngine(QueueConfig(), clock)
    delayed = engine.enqueue({"name": "delayed"}, delay_seconds=5)

    assert engine.receive("worker-1") is None
    clock.advance(5)
    first_lease = require_message(
        engine.receive("worker-1", visibility_timeout_seconds=3)
    )
    assert first_lease.id == delayed.id
    assert first_lease.receipt_handle is not None

    clock.advance(3)
    second_lease = require_message(engine.receive("worker-2"))
    assert second_lease.id == delayed.id
    assert second_lease.delivery_attempts == 2
    assert second_lease.receipt_handle != first_lease.receipt_handle

    with pytest.raises(InvalidReceiptHandleError):
        engine.ack(delayed.id, first_lease.receipt_handle)
