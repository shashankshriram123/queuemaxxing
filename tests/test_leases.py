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


def stored(engine: QueueEngine, message: Message) -> Message:
    return engine.get_message(message.id)


def test_receive_chooses_the_correct_next_message(clock: FakeClock) -> None:
    engine = QueueEngine(
        QueueConfig(order=QueueOrder.FIFO, priority_enabled=True), clock
    )
    engine.enqueue({"name": "old-low"}, priority=1)
    expected = engine.enqueue({"name": "old-high"}, priority=10)
    engine.enqueue({"name": "new-high"}, priority=10)

    assert require_message(engine.receive("worker-1")).id == expected.id


def test_receive_creates_an_active_lease(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})

    received = require_message(
        engine.receive("  worker-7  ", visibility_timeout_seconds=12)
    )

    assert received.id == message.id
    assert received.state is MessageState.IN_FLIGHT
    assert received.leased_by == "worker-7"
    assert isinstance(received.receipt_handle, str)
    assert len(received.receipt_handle) >= 32
    assert received.lease_expires_at == clock.current + timedelta(seconds=12)
    assert received.delivery_attempts == 1
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

    assert result.id == message.id
    assert result.state is MessageState.COMPLETED
    assert result.completed_at == clock.current
    assert result.leased_by is None
    assert result.receipt_handle is None
    assert result.lease_expires_at is None
    assert stored(engine, message) == result


def test_completed_message_is_never_received_again(engine: QueueEngine) -> None:
    message = engine.enqueue({"name": "job"})
    received = require_message(engine.receive("worker-1"))
    receipt_handle = received.receipt_handle
    assert receipt_handle is not None
    engine.ack(message.id, receipt_handle)

    assert engine.peek() is None
    assert engine.ready_messages() == []
    assert engine.receive("worker-2") is None


def test_clear_completed_removes_only_completed_messages(engine: QueueEngine) -> None:
    first = engine.enqueue({"name": "done-1"})
    first_lease = require_message(engine.receive("worker-1"))
    assert first_lease.receipt_handle is not None
    engine.ack(first.id, first_lease.receipt_handle)

    second = engine.enqueue({"name": "done-2"})
    second_lease = require_message(engine.receive("worker-2"))
    assert second_lease.receipt_handle is not None
    engine.ack(second.id, second_lease.receipt_handle)
    ready = engine.enqueue({"name": "keep"})

    assert engine.clear_completed() == 2
    assert [message.id for message in engine.messages()] == [ready.id]
    with pytest.raises(MessageNotFoundError):
        engine.get_message(first.id)
    with pytest.raises(MessageNotFoundError):
        engine.get_message(second.id)
    assert engine.clear_completed() == 0


def test_nack_without_delay_returns_message_to_ready(engine: QueueEngine) -> None:
    message = engine.enqueue({"name": "job"})
    received = require_message(engine.receive("worker-1"))
    receipt_handle = received.receipt_handle
    assert receipt_handle is not None

    result = engine.nack(message.id, receipt_handle)

    assert result.id == message.id
    assert result.state is MessageState.READY
    assert result.delivery_attempts == 1
    assert result.leased_by is None
    assert result.receipt_handle is None
    assert result.lease_expires_at is None
    assert require_message(engine.peek_next()).id == message.id


def test_nack_with_retry_delay_moves_message_to_delayed(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})
    received = require_message(engine.receive("worker-1"))
    receipt_handle = received.receipt_handle
    assert receipt_handle is not None

    engine.nack(message.id, receipt_handle, retry_delay_seconds=8)

    retry = stored(engine, message)
    assert retry.state is MessageState.DELAYED
    assert retry.available_at == clock.current + timedelta(seconds=8)
    assert retry.delivery_attempts == 1
    assert engine.receive("worker-2") is None

    clock.advance(8)

    assert require_message(engine.peek_next()).id == message.id
    assert stored(engine, message).state is MessageState.READY


def test_expired_lease_returns_message_to_ready(
    engine: QueueEngine, clock: FakeClock
) -> None:
    message = engine.enqueue({"name": "job"})
    engine.receive("worker-1", visibility_timeout_seconds=5)

    clock.advance(5)

    assert engine.requeue_expired_leases() == 1
    current = stored(engine, message)
    assert current.state is MessageState.READY
    assert current.delivery_attempts == 1
    assert current.leased_by is None
    assert current.receipt_handle is None
    assert current.lease_expires_at is None


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

    assert second_delivery.id == message.id
    assert second_delivery.delivery_attempts == 2
    assert second_delivery.receipt_handle is not None
    assert second_delivery.receipt_handle != first_receipt
    assert second_delivery.leased_by == "worker-2"


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

    assert require_message(engine.peek()).id == message.id
    assert require_message(engine.peek_next()).id == message.id
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
    assert {
        stored(engine, first).state,
        stored(engine, second).state,
    } == {MessageState.READY}


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

    assert redelivered.id == high.id
    assert redelivered.delivery_attempts == 2
