"""In-memory queue ordering and delay semantics."""

from __future__ import annotations

import copy
import math
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from app.models import JSONValue, Message, MessageState, QueueConfig, QueueOrder


Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validate_json_value(value: object, path: str = "payload") -> None:
    """Reject values that cannot be represented faithfully as JSON."""

    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} cannot contain NaN or infinity")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} object keys must be strings")
            _validate_json_value(item, f"{path}.{key}")
        return
    raise TypeError(f"{path} must be JSON-compatible")


class QueueEngine:
    """Own messages and determine which eligible message should run next."""

    def __init__(self, config: QueueConfig, clock: Clock | None = None) -> None:
        if not isinstance(config, QueueConfig):
            raise TypeError("config must be a QueueConfig")

        self.config = config
        self._clock = clock or _utc_now
        self._messages: dict[str, Message] = {}
        self._last_sequence = 0

    def enqueue(
        self,
        payload: JSONValue,
        *,
        priority: int = 0,
        delay_seconds: float = 0,
    ) -> Message:
        """Add a message and return it without changing queue order."""

        if type(priority) is not int:
            raise TypeError("priority must be an integer")
        if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, (int, float)):
            raise TypeError("delay_seconds must be a number")
        if not math.isfinite(delay_seconds):
            raise ValueError("delay_seconds must be finite")
        if delay_seconds < 0:
            raise ValueError("delay_seconds cannot be negative")

        _validate_json_value(payload)
        now = self._now()
        self._last_sequence += 1
        message = Message(
            id=str(uuid.uuid4()),
            payload=copy.deepcopy(payload),
            priority=priority,
            sequence=self._last_sequence,
            created_at=now,
            available_at=now + timedelta(seconds=delay_seconds),
            state=MessageState.DELAYED if delay_seconds > 0 else MessageState.READY,
        )
        self._messages[message.id] = message
        return message

    def ready_messages(self) -> list[Message]:
        """Return all eligible messages in their processing order."""

        self._promote_delayed()
        ready = [
            message
            for message in self._messages.values()
            if message.state is MessageState.READY
        ]
        return sorted(ready, key=self._ordering_key)

    def peek_next(self) -> Message | None:
        """Return the next eligible message without removing or claiming it."""

        ready = self.ready_messages()
        return ready[0] if ready else None

    def _promote_delayed(self) -> None:
        now = self._now()
        for message in self._messages.values():
            if (
                message.state is MessageState.DELAYED
                and message.available_at <= now
            ):
                message.state = MessageState.READY

    def _ordering_key(self, message: Message) -> tuple[int, int]:
        priority = -message.priority if self.config.priority_enabled else 0
        sequence = (
            message.sequence
            if self.config.order is QueueOrder.FIFO
            else -message.sequence
        )
        return priority, sequence

    def _now(self) -> datetime:
        now = self._clock()
        if not isinstance(now, datetime):
            raise TypeError("clock must return a datetime")
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return now
