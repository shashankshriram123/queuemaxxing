from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from app.models import MessageState, QueueConfig, QueueOrder
from app.queue_engine import QueueEngine


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


def payload_names(messages: list) -> list[str]:
    return [message.payload["name"] for message in messages]


def test_fifo_returns_oldest_messages_first(clock: FakeClock) -> None:
    engine = QueueEngine(QueueConfig(order=QueueOrder.FIFO), clock)
    engine.enqueue({"name": "first"})
    engine.enqueue({"name": "second"})
    engine.enqueue({"name": "third"})

    assert payload_names(engine.ready_messages()) == ["first", "second", "third"]


def test_lifo_returns_newest_messages_first(clock: FakeClock) -> None:
    engine = QueueEngine(QueueConfig(order=QueueOrder.LIFO), clock)
    engine.enqueue({"name": "first"})
    engine.enqueue({"name": "second"})
    engine.enqueue({"name": "third"})

    assert payload_names(engine.ready_messages()) == ["third", "second", "first"]


def test_priority_fifo_uses_priority_then_oldest_for_ties(clock: FakeClock) -> None:
    engine = QueueEngine(
        QueueConfig(order=QueueOrder.FIFO, priority_enabled=True), clock
    )
    engine.enqueue({"name": "low"}, priority=1)
    engine.enqueue({"name": "high-oldest"}, priority=10)
    engine.enqueue({"name": "high-newest"}, priority=10)

    assert payload_names(engine.ready_messages()) == [
        "high-oldest",
        "high-newest",
        "low",
    ]


def test_priority_lifo_uses_priority_then_newest_for_ties(clock: FakeClock) -> None:
    engine = QueueEngine(
        QueueConfig(order=QueueOrder.LIFO, priority_enabled=True), clock
    )
    engine.enqueue({"name": "low"}, priority=1)
    engine.enqueue({"name": "high-oldest"}, priority=10)
    engine.enqueue({"name": "high-newest"}, priority=10)

    assert payload_names(engine.ready_messages()) == [
        "high-newest",
        "high-oldest",
        "low",
    ]


def test_priority_is_ignored_when_disabled(clock: FakeClock) -> None:
    engine = QueueEngine(
        QueueConfig(order=QueueOrder.FIFO, priority_enabled=False), clock
    )
    engine.enqueue({"name": "old-low"}, priority=-100)
    engine.enqueue({"name": "new-high"}, priority=100)

    assert payload_names(engine.ready_messages()) == ["old-low", "new-high"]


def test_delayed_message_is_unavailable_before_scheduled_time(
    clock: FakeClock,
) -> None:
    engine = QueueEngine(QueueConfig(), clock)
    message = engine.enqueue({"name": "later"}, delay_seconds=10)

    assert message.state is MessageState.DELAYED
    assert engine.ready_messages() == []
    assert engine.peek_next() is None


def test_delayed_message_becomes_ready_after_clock_advances(clock: FakeClock) -> None:
    engine = QueueEngine(QueueConfig(), clock)
    message = engine.enqueue({"name": "later"}, delay_seconds=10)

    clock.advance(10)

    assert engine.ready_messages() == [message]
    assert message.state is MessageState.READY


@pytest.mark.parametrize(
    ("order", "expected"),
    [
        (QueueOrder.FIFO, ["delayed-oldest", "ready-newest"]),
        (QueueOrder.LIFO, ["ready-newest", "delayed-oldest"]),
    ],
)
def test_newly_eligible_message_uses_normal_tie_breaking(
    clock: FakeClock, order: QueueOrder, expected: list[str]
) -> None:
    engine = QueueEngine(QueueConfig(order=order, priority_enabled=True), clock)
    engine.enqueue({"name": "delayed-oldest"}, priority=10, delay_seconds=5)
    engine.enqueue({"name": "ready-newest"}, priority=10)

    clock.advance(5)

    assert payload_names(engine.ready_messages()) == expected


def test_newly_eligible_high_priority_message_moves_ahead(clock: FakeClock) -> None:
    engine = QueueEngine(
        QueueConfig(order=QueueOrder.FIFO, priority_enabled=True), clock
    )
    engine.enqueue({"name": "delayed-vip"}, priority=50, delay_seconds=5)
    engine.enqueue({"name": "ready-standard"}, priority=1)

    assert payload_names(engine.ready_messages()) == ["ready-standard"]

    clock.advance(5)

    assert payload_names(engine.ready_messages()) == [
        "delayed-vip",
        "ready-standard",
    ]


def test_peek_does_not_remove_or_mutate_selected_message(clock: FakeClock) -> None:
    engine = QueueEngine(QueueConfig(), clock)
    message = engine.enqueue({"name": "first", "nested": [1, 2, 3]})
    engine.enqueue({"name": "second"})
    before = deepcopy(message)

    first_peek = engine.peek_next()
    second_peek = engine.peek_next()

    assert first_peek is message
    assert second_peek is message
    assert message == before
    assert len(engine.ready_messages()) == 2


def test_empty_queue_has_no_next_message(clock: FakeClock) -> None:
    engine = QueueEngine(QueueConfig(), clock)

    assert engine.peek_next() is None
    assert engine.ready_messages() == []


def test_negative_delay_is_rejected(clock: FakeClock) -> None:
    engine = QueueEngine(QueueConfig(), clock)

    with pytest.raises(ValueError, match="cannot be negative"):
        engine.enqueue({"name": "invalid"}, delay_seconds=-1)


def test_non_integer_priority_is_rejected(clock: FakeClock) -> None:
    engine = QueueEngine(QueueConfig(), clock)

    with pytest.raises(TypeError, match="must be an integer"):
        engine.enqueue({"name": "invalid"}, priority=1.5)  # type: ignore[arg-type]


def test_unsupported_queue_order_is_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported queue order"):
        QueueConfig(order="random")  # type: ignore[arg-type]


def test_messages_receive_distinct_ids_and_sequences(clock: FakeClock) -> None:
    engine = QueueEngine(QueueConfig(), clock)

    messages = [engine.enqueue({"name": str(index)}) for index in range(3)]

    assert len({message.id for message in messages}) == 3
    assert [message.sequence for message in messages] == [1, 2, 3]
