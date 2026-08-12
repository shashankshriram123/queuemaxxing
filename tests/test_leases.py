from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from app.models import Message, MessageState, QueueConfig, QueueOrder
from app.queue_engine import (
    InvalidReceiptHandleError,
    InvalidRetryDelayError,
    InvalidVisibilityTimeoutError,
    InvalidWorkerIDError,
    MessageNotFoundError,
    MessageNotInFlightError,
    QueueEngine,
)


@dataclass
class FakeClock:
    current: datetime

    def __call__(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(datetime(2026, 1, 1, tzinfo=timezone.utc))


@pytest.fixture
def engine(clock: FakeClock) -> QueueEngine:
    return QueueEngine(QueueConfig(), clock)


def require_message(message: Message | None) -> Message:
    assert message is not None
    return message


def test_receive_chooses_the_correct_next_message(clock: FakeClock) -> None:
    engine = QueueEngine(
        QueueConfig(order=QueueOrder.FIFO, priority_enabled=True), clock
    )
    engine.enqueue({"name": "old-low"}, priority=1)
    expected = engine.enqueue({"name": "old-high"}, priority=10)
    engine.enqueue({"name": "new-high"}, priority=10)

    assert engine.receive("worker-1") is expected


def test_receive_creates_an_active_lease(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})

    received = require_message(
        engine.receive("  worker-7  ", visibility_timeout_seconds=12)
    )

    assert received is message
    assert message.state is MessageState.IN_FLIGHT
    assert message.leased_by == "worker-7"
    assert isinstance(message.receipt_handle, str)
    assert len(message.receipt_handle) >= 32
    assert message.lease_expires_at == clock.current + timedelta(seconds=12)
    assert message.delivery_attempts == 1
    assert engine.ready_messages() == []


def test_in_flight_message_cannot_be_received_by_another_worker(
    engine: QueueEngine,
) -> None:
    engine.enqueue({"name": "only-job"})

    assert engine.receive("worker-1") is not None
    assert engine.receive("worker-2") is None


def test_ack_completes_message_and_clears_active_lease(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})
    received = require_message(engine.receive("worker-1"))
    receipt_handle = received.receipt_handle
    assert receipt_handle is not None

    result = engine.ack(message.id, receipt_handle)

    assert result is message
    assert message.state is MessageState.COMPLETED
    assert message.completed_at == clock.current
    assert message.leased_by is None
    assert message.receipt_handle is None
    assert message.lease_expires_at is None


def test_completed_message_is_never_received_again(engine: QueueEngine) -> None:
    message = engine.enqueue({"name": "job"})
    received = require_message(engine.receive("worker-1"))
    receipt_handle = received.receipt_handle
    assert receipt_handle is not None
    engine.ack(message.id, receipt_handle)

    assert engine.peek() is None
    assert engine.ready_messages() == []
    assert engine.receive("worker-2") is None


def test_nack_without_delay_returns_message_to_ready(engine: QueueEngine) -> None:
    message = engine.enqueue({"name": "job"})
    received = require_message(engine.receive("worker-1"))
    receipt_handle = received.receipt_handle
    assert receipt_handle is not None

    result = engine.nack(message.id, receipt_handle)

    assert result is message
    assert message.state is MessageState.READY
    assert message.delivery_attempts == 1
    assert message.leased_by is None
    assert message.receipt_handle is None
    assert message.lease_expires_at is None
    assert engine.peek_next() is message


def test_nack_with_retry_delay_moves_message_to_delayed(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})
    received = require_message(engine.receive("worker-1"))
    receipt_handle = received.receipt_handle
    assert receipt_handle is not None

    engine.nack(message.id, receipt_handle, retry_delay_seconds=8)

    assert message.state is MessageState.DELAYED
    assert message.available_at == clock.current + timedelta(seconds=8)
    assert message.delivery_attempts == 1
    assert engine.receive("worker-2") is None

    clock.advance(8)

    assert engine.peek_next() is message
    assert message.state is MessageState.READY


def test_expired_lease_returns_message_to_ready(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})
    engine.receive("worker-1", visibility_timeout_seconds=5)

    clock.advance(5)

    assert engine.requeue_expired_leases() == 1
    assert message.state is MessageState.READY
    assert message.delivery_attempts == 1
    assert message.leased_by is None
    assert message.receipt_handle is None
    assert message.lease_expires_at is None


def test_redelivery_increments_attempts_and_creates_a_new_receipt(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})
    first_delivery = require_message(
        engine.receive("worker-1", visibility_timeout_seconds=5)
    )
    first_receipt = first_delivery.receipt_handle
    assert first_receipt is not None

    clock.advance(5)
    second_delivery = require_message(engine.receive("worker-2"))

    assert second_delivery is message
    assert message.delivery_attempts == 2
    assert message.receipt_handle is not None
    assert message.receipt_handle != first_receipt
    assert message.leased_by == "worker-2"


def test_old_receipt_cannot_ack_a_newer_lease(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})
    first = require_message(engine.receive("worker-1", visibility_timeout_seconds=2))
    old_receipt = first.receipt_handle
    assert old_receipt is not None
    clock.advance(2)
    second = require_message(engine.receive("worker-2"))
    assert second.receipt_handle != old_receipt

    with pytest.raises(InvalidReceiptHandleError):
        engine.ack(message.id, old_receipt)


def test_old_receipt_cannot_nack_a_newer_lease(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})
    first = require_message(engine.receive("worker-1", visibility_timeout_seconds=2))
    old_receipt = first.receipt_handle
    assert old_receipt is not None
    clock.advance(2)
    second = require_message(engine.receive("worker-2"))
    assert second.receipt_handle != old_receipt

    with pytest.raises(InvalidReceiptHandleError):
        engine.nack(message.id, old_receipt)


def test_ack_with_unknown_message_id_is_rejected(engine: QueueEngine) -> None:
    with pytest.raises(MessageNotFoundError):
        engine.ack("missing", "receipt")


def test_nack_with_unknown_message_id_is_rejected(engine: QueueEngine) -> None:
    with pytest.raises(MessageNotFoundError):
        engine.nack("missing", "receipt")


def test_ack_for_ready_message_is_rejected(engine: QueueEngine) -> None:
    message = engine.enqueue({"name": "job"})

    with pytest.raises(MessageNotInFlightError):
        engine.ack(message.id, "receipt")


def test_nack_for_completed_message_is_rejected(engine: QueueEngine) -> None:
    message = engine.enqueue({"name": "job"})
    received = require_message(engine.receive("worker-1"))
    receipt_handle = received.receipt_handle
    assert receipt_handle is not None
    engine.ack(message.id, receipt_handle)

    with pytest.raises(MessageNotInFlightError):
        engine.nack(message.id, receipt_handle)


@pytest.mark.parametrize("worker_id", ["", " ", "\t"])
def test_blank_worker_ids_are_rejected(
    engine: QueueEngine, worker_id: str
) -> None:
    with pytest.raises(InvalidWorkerIDError):
        engine.receive(worker_id)


@pytest.mark.parametrize("timeout", [0, -1])
def test_non_positive_visibility_timeouts_are_rejected(
    engine: QueueEngine, timeout: float
) -> None:
    with pytest.raises(InvalidVisibilityTimeoutError):
        engine.receive("worker-1", visibility_timeout_seconds=timeout)


def test_negative_retry_delay_is_rejected(engine: QueueEngine) -> None:
    message = engine.enqueue({"name": "job"})
    received = require_message(engine.receive("worker-1"))
    receipt_handle = received.receipt_handle
    assert receipt_handle is not None

    with pytest.raises(InvalidRetryDelayError):
        engine.nack(message.id, receipt_handle, retry_delay_seconds=-1)


def test_peek_does_not_lease_or_mutate_message(engine: QueueEngine) -> None:
    message = engine.enqueue({"name": "job"})
    before = deepcopy(message)

    assert engine.peek() is message
    assert engine.peek_next() is message
    assert message == before
    assert message.state is MessageState.READY
    assert message.delivery_attempts == 0
    assert message.receipt_handle is None


@pytest.mark.parametrize(
    ("config", "names"),
    [
        (QueueConfig(order=QueueOrder.FIFO), ["first", "second"]),
        (QueueConfig(order=QueueOrder.LIFO), ["second", "first"]),
        (
            QueueConfig(order=QueueOrder.FIFO, priority_enabled=True),
            ["first", "second"],
        ),
    ],
)
def test_ordering_is_preserved_across_nack_and_redelivery(
    clock: FakeClock, config: QueueConfig, names: list[str]
) -> None:
    engine = QueueEngine(config, clock)
    first = engine.enqueue({"name": "first"}, priority=10)
    second = engine.enqueue({"name": "second"}, priority=1)
    selected = require_message(engine.receive("worker-1"))
    receipt_handle = selected.receipt_handle
    assert receipt_handle is not None

    engine.nack(selected.id, receipt_handle)

    assert [message.payload["name"] for message in engine.ready_messages()] == names
    assert {first.state, second.state} == {MessageState.READY}


def test_priority_ordering_is_preserved_after_lease_expiration(
    clock: FakeClock,
) -> None:
    engine = QueueEngine(
        QueueConfig(order=QueueOrder.LIFO, priority_enabled=True), clock
    )
    high = engine.enqueue({"name": "high"}, priority=10)
    engine.enqueue({"name": "low"}, priority=1)
    engine.receive("worker-1", visibility_timeout_seconds=3)
    clock.advance(3)

    redelivered = require_message(engine.receive("worker-2"))

    assert redelivered is high
    assert redelivered.delivery_attempts == 2
