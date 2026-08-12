"""In-memory queue ordering and delay semantics."""

from __future__ import annotations

import copy
import math
import secrets
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from app.models import JSONValue, Message, MessageState, QueueConfig, QueueOrder


Clock = Callable[[], datetime]


class QueueEngineError(Exception):
    """Base class for queue lifecycle errors."""


class MessageNotFoundError(QueueEngineError):
    """Raised when a message ID is not owned by this engine."""


class MessageNotInFlightError(QueueEngineError):
    """Raised when an operation requires an active message lease."""


class InvalidReceiptHandleError(QueueEngineError):
    """Raised when a receipt does not match the active message lease."""


class InvalidVisibilityTimeoutError(QueueEngineError, ValueError):
    """Raised when a visibility timeout is not positive and finite."""


class InvalidRetryDelayError(QueueEngineError, ValueError):
    """Raised when a retry delay is negative or non-finite."""


class InvalidWorkerIDError(QueueEngineError, ValueError):
    """Raised when a receive operation has no usable worker ID."""


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

        self._refresh_time_based_states(self._now())
        return self._ordered_ready_messages()

    def peek_next(self) -> Message | None:
        """Return the next eligible message without removing or claiming it."""

        ready = self.ready_messages()
        return ready[0] if ready else None

    def peek(self) -> Message | None:
        """Alias for :meth:`peek_next` with the same non-destructive behavior."""

        return self.peek_next()

    def receive(
        self,
        worker_id: str,
        visibility_timeout_seconds: float = 30,
    ) -> Message | None:
        """Lease and return the next eligible message, if one exists."""

        normalized_worker_id = self._validate_worker_id(worker_id)
        visibility_timeout = self._validate_visibility_timeout(
            visibility_timeout_seconds
        )
        now = self._now()
        self._refresh_time_based_states(now)
        ready = self._ordered_ready_messages()
        if not ready:
            return None

        message = ready[0]
        message.state = MessageState.IN_FLIGHT
        message.delivery_attempts += 1
        message.leased_by = normalized_worker_id
        message.receipt_handle = secrets.token_urlsafe(32)
        message.lease_expires_at = now + timedelta(seconds=visibility_timeout)
        return message

    def ack(self, message_id: str, receipt_handle: str) -> Message:
        """Complete a message using the receipt for its active lease."""

        now = self._now()
        self._refresh_time_based_states(now)
        message = self._get_message(message_id)
        self._validate_active_lease(message, receipt_handle)

        message.state = MessageState.COMPLETED
        message.completed_at = now
        self._clear_active_lease(message)
        return message

    def nack(
        self,
        message_id: str,
        receipt_handle: str,
        retry_delay_seconds: float = 0,
    ) -> Message:
        """Release an active lease immediately or after a retry delay."""

        retry_delay = self._validate_retry_delay(retry_delay_seconds)
        now = self._now()
        self._refresh_time_based_states(now)
        message = self._get_message(message_id)
        self._validate_active_lease(message, receipt_handle)

        message.available_at = now + timedelta(seconds=retry_delay)
        message.state = (
            MessageState.DELAYED if retry_delay > 0 else MessageState.READY
        )
        self._clear_active_lease(message)
        return message

    def requeue_expired_leases(self) -> int:
        """Return expired in-flight messages to ready and report the count."""

        return self._requeue_expired_leases_at(self._now())

    def _refresh_time_based_states(self, now: datetime) -> None:
        self._requeue_expired_leases_at(now)
        self._promote_delayed_at(now)

    def _promote_delayed_at(self, now: datetime) -> None:
        for message in self._messages.values():
            if (
                message.state is MessageState.DELAYED
                and message.available_at <= now
            ):
                message.state = MessageState.READY

    def _requeue_expired_leases_at(self, now: datetime) -> int:
        expired_count = 0
        for message in self._messages.values():
            if (
                message.state is MessageState.IN_FLIGHT
                and message.lease_expires_at is not None
                and message.lease_expires_at <= now
            ):
                message.state = MessageState.READY
                self._clear_active_lease(message)
                expired_count += 1
        return expired_count

    def _ordered_ready_messages(self) -> list[Message]:
        ready = [
            message
            for message in self._messages.values()
            if message.state is MessageState.READY
        ]
        return sorted(ready, key=self._ordering_key)

    def _ordering_key(self, message: Message) -> tuple[int, int]:
        priority = -message.priority if self.config.priority_enabled else 0
        sequence = (
            message.sequence
            if self.config.order is QueueOrder.FIFO
            else -message.sequence
        )
        return priority, sequence

    def _get_message(self, message_id: str) -> Message:
        try:
            return self._messages[message_id]
        except KeyError as exc:
            raise MessageNotFoundError(f"unknown message ID: {message_id}") from exc

    @staticmethod
    def _validate_active_lease(message: Message, receipt_handle: str) -> None:
        if message.state is not MessageState.IN_FLIGHT:
            raise MessageNotInFlightError(
                f"message {message.id} is not currently in flight"
            )
        if (
            not isinstance(receipt_handle, str)
            or message.receipt_handle is None
            or not secrets.compare_digest(message.receipt_handle, receipt_handle)
        ):
            raise InvalidReceiptHandleError(
                f"receipt handle does not match the active lease for {message.id}"
            )

    @staticmethod
    def _clear_active_lease(message: Message) -> None:
        message.leased_by = None
        message.receipt_handle = None
        message.lease_expires_at = None

    @staticmethod
    def _validate_worker_id(worker_id: str) -> str:
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise InvalidWorkerIDError("worker_id must be a non-blank string")
        return worker_id.strip()

    @staticmethod
    def _validate_visibility_timeout(value: float) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise InvalidVisibilityTimeoutError(
                "visibility_timeout_seconds must be a number"
            )
        if not math.isfinite(value) or value <= 0:
            raise InvalidVisibilityTimeoutError(
                "visibility_timeout_seconds must be positive and finite"
            )
        return float(value)

    @staticmethod
    def _validate_retry_delay(value: float) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise InvalidRetryDelayError("retry_delay_seconds must be a number")
        if not math.isfinite(value) or value < 0:
            raise InvalidRetryDelayError(
                "retry_delay_seconds must be non-negative and finite"
            )
        return float(value)

    def _now(self) -> datetime:
        now = self._clock()
        if not isinstance(now, datetime):
            raise TypeError("clock must return a datetime")
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return now
