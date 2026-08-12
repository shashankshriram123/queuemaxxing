"""Thread-safe queue lifecycle, ordering, and optional WAL durability."""

from __future__ import annotations

import copy
import math
import secrets
import threading
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.models import JSONValue, Message, MessageState, QueueConfig, QueueOrder
from app.wal import WALCorruptionError, WALRecord, WriteAheadLog


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


class QueueConfigurationMismatchError(QueueEngineError):
    """Raised when requested settings conflict with durable configuration."""


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
    """Own messages and atomically coordinate concurrent producer/consumer threads."""

    def __init__(
        self,
        config: QueueConfig | None = None,
        clock: Clock | None = None,
        wal: WriteAheadLog | None = None,
    ) -> None:
        if config is not None and not isinstance(config, QueueConfig):
            raise TypeError("config must be a QueueConfig")
        if wal is not None and not isinstance(wal, WriteAheadLog):
            raise TypeError("wal must be a WriteAheadLog")

        requested_config = config
        self.config = config or QueueConfig()
        self._clock = clock or _utc_now
        self._messages: dict[str, Message] = {}
        self._last_sequence = 0
        self._lock = threading.RLock()
        self._wal = wal
        self._configuration_loaded = wal is None

        if wal is not None:
            with self._lock:
                self._recover_locked(requested_config)

    @classmethod
    def open_durable(
        cls,
        wal_path: str | Path,
        config: QueueConfig | None = None,
        clock: Clock | None = None,
    ) -> "QueueEngine":
        """Open a single-owner durable engine and recover its stored state."""

        wal = WriteAheadLog(wal_path)
        try:
            return cls(config=config, clock=clock, wal=wal)
        except Exception:
            wal.close()
            raise

    def close(self) -> None:
        """Release durable WAL ownership; in-memory engines need no teardown."""

        with self._lock:
            if self._wal is not None:
                self._wal.close()

    def __enter__(self) -> "QueueEngine":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

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

        payload_copy = copy.deepcopy(payload)
        _validate_json_value(payload_copy)

        with self._lock:
            now = self._now()
            message_id = str(uuid.uuid4())
            initial_state = (
                MessageState.DELAYED if delay_seconds > 0 else MessageState.READY
            )
            event_data = {
                "message_id": message_id,
                "payload": payload_copy,
                "priority": priority,
                "sequence": self._last_sequence + 1,
                "created_at": self._serialize_timestamp(now),
                "available_at": self._serialize_timestamp(
                    now + timedelta(seconds=delay_seconds)
                ),
                "state": initial_state.value,
            }
            self._commit_event_locked("message_enqueued", event_data)
            return self._snapshot_locked(self._messages[message_id])

    def ready_messages(self) -> list[Message]:
        """Return all eligible messages in their processing order."""

        with self._lock:
            self._refresh_time_based_states_locked(self._now())
            return [
                self._snapshot_locked(message)
                for message in self._ordered_ready_messages_locked()
            ]

    def peek_next(self) -> Message | None:
        """Return the next eligible message without removing or claiming it."""

        with self._lock:
            self._refresh_time_based_states_locked(self._now())
            ready = self._ordered_ready_messages_locked()
            return self._snapshot_locked(ready[0]) if ready else None

    def peek(self) -> Message | None:
        """Alias for :meth:`peek_next` with the same non-destructive behavior."""

        with self._lock:
            return self.peek_next()

    def get_message(self, message_id: str) -> Message:
        """Return a defensive snapshot of one message's current state."""

        with self._lock:
            self._refresh_time_based_states_locked(self._now())
            return self._snapshot_locked(self._get_message_locked(message_id))

    def messages(self) -> list[Message]:
        """Return defensive snapshots of all messages in sequence order."""

        with self._lock:
            self._refresh_time_based_states_locked(self._now())
            ordered = sorted(self._messages.values(), key=lambda item: item.sequence)
            return [self._snapshot_locked(message) for message in ordered]

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
        with self._lock:
            now = self._now()
            self._refresh_time_based_states_locked(now)
            ready = self._ordered_ready_messages_locked()
            if not ready:
                return None

            message = ready[0]
            event_data = {
                "message_id": message.id,
                "worker_id": normalized_worker_id,
                "receipt_handle": secrets.token_urlsafe(32),
                "lease_expires_at": self._serialize_timestamp(
                    now + timedelta(seconds=visibility_timeout)
                ),
                "delivery_attempts": message.delivery_attempts + 1,
            }
            self._commit_event_locked("message_claimed", event_data)
            return self._snapshot_locked(message)

    def ack(self, message_id: str, receipt_handle: str) -> Message:
        """Complete a message using the receipt for its active lease."""

        with self._lock:
            now = self._now()
            self._refresh_time_based_states_locked(now)
            message = self._get_message_locked(message_id)
            self._validate_active_lease(message, receipt_handle)

            self._commit_event_locked(
                "message_acked",
                {
                    "message_id": message.id,
                    "completed_at": self._serialize_timestamp(now),
                },
            )
            return self._snapshot_locked(message)

    def nack(
        self,
        message_id: str,
        receipt_handle: str,
        retry_delay_seconds: float = 0,
    ) -> Message:
        """Release an active lease immediately or after a retry delay."""

        retry_delay = self._validate_retry_delay(retry_delay_seconds)
        with self._lock:
            now = self._now()
            self._refresh_time_based_states_locked(now)
            message = self._get_message_locked(message_id)
            self._validate_active_lease(message, receipt_handle)

            retry_state = (
                MessageState.DELAYED if retry_delay > 0 else MessageState.READY
            )
            self._commit_event_locked(
                "message_nacked",
                {
                    "message_id": message.id,
                    "state": retry_state.value,
                    "available_at": self._serialize_timestamp(
                        now + timedelta(seconds=retry_delay)
                    ),
                },
            )
            return self._snapshot_locked(message)

    def requeue_expired_leases(self) -> int:
        """Return expired in-flight messages to ready and report the count."""

        with self._lock:
            return self._requeue_expired_leases_locked(self._now())

    def _refresh_time_based_states_locked(self, now: datetime) -> None:
        self._requeue_expired_leases_locked(now)
        self._promote_delayed_locked(now)

    def _promote_delayed_locked(self, now: datetime) -> None:
        for message in self._messages.values():
            if (
                message.state is MessageState.DELAYED
                and message.available_at <= now
            ):
                message.state = MessageState.READY

    def _requeue_expired_leases_locked(self, now: datetime) -> int:
        expired = [
            message
            for message in self._messages.values()
            if message.state is MessageState.IN_FLIGHT
            and message.lease_expires_at is not None
            and message.lease_expires_at <= now
        ]
        for message in expired:
            receipt_handle = message.receipt_handle
            if receipt_handle is None:
                raise RuntimeError("in-flight message is missing its receipt handle")
            self._commit_event_locked(
                "lease_expired",
                {
                    "message_id": message.id,
                    "receipt_handle": receipt_handle,
                    "available_at": self._serialize_timestamp(now),
                },
            )
        return len(expired)

    def _recover_locked(self, requested_config: QueueConfig | None) -> None:
        if self._wal is None:
            raise RuntimeError("recovery requires a WAL")

        records = self._wal.records
        if not records:
            initial_config = requested_config or QueueConfig()
            self._commit_event_locked(
                "queue_configured",
                {
                    "order": initial_config.order.value,
                    "priority_enabled": initial_config.priority_enabled,
                },
            )
        else:
            if records[0].event_type != "queue_configured":
                raise WALCorruptionError(
                    "the first WAL event must configure the queue"
                )
            for record in records:
                self._apply_record_locked(record)

            if requested_config is not None and requested_config != self.config:
                raise QueueConfigurationMismatchError(
                    "requested queue configuration conflicts with the WAL"
                )

        now = self._now()
        self._requeue_expired_leases_locked(now)
        self._promote_delayed_locked(now)

    def _commit_event_locked(
        self, event_type: str, data: dict[str, Any]
    ) -> None:
        """Durably append an event before applying it to memory."""

        if self._wal is not None:
            record = self._wal.append(event_type, data)
        else:
            record = WALRecord(
                version=1,
                record_number=0,
                event_type=event_type,
                data=copy.deepcopy(data),
                checksum="",
            )
        self._apply_record_locked(record)

    def _apply_record_locked(self, record: WALRecord) -> None:
        """Apply one already-validated event without writing another event."""

        data = record.data
        try:
            if record.event_type == "queue_configured":
                if self._configuration_loaded:
                    raise WALCorruptionError("queue was configured more than once")
                self.config = QueueConfig(
                    order=QueueOrder(data["order"]),
                    priority_enabled=data["priority_enabled"],
                )
                self._configuration_loaded = True
                return

            if not self._configuration_loaded:
                raise WALCorruptionError(
                    "queue transition appeared before queue configuration"
                )

            message_id = data["message_id"]
            if record.event_type == "message_enqueued":
                if message_id in self._messages:
                    raise WALCorruptionError(f"duplicate message ID: {message_id}")
                sequence = data["sequence"]
                if sequence != self._last_sequence + 1:
                    raise WALCorruptionError(
                        "message sequence numbers must increase without reuse"
                    )
                message = Message(
                    id=message_id,
                    payload=copy.deepcopy(data["payload"]),
                    priority=data["priority"],
                    sequence=sequence,
                    created_at=self._parse_timestamp(data["created_at"]),
                    available_at=self._parse_timestamp(data["available_at"]),
                    state=MessageState(data["state"]),
                )
                self._messages[message.id] = message
                self._last_sequence = sequence
            elif record.event_type == "message_claimed":
                message = self._message_for_replay_locked(message_id)
                if message.state in {MessageState.IN_FLIGHT, MessageState.COMPLETED}:
                    raise WALCorruptionError(
                        f"message {message_id} cannot be claimed from {message.state.value}"
                    )
                if data["delivery_attempts"] != message.delivery_attempts + 1:
                    raise WALCorruptionError(
                        f"message {message_id} has invalid delivery-attempt progression"
                    )
                message.state = MessageState.IN_FLIGHT
                message.delivery_attempts = data["delivery_attempts"]
                message.leased_by = data["worker_id"]
                message.receipt_handle = data["receipt_handle"]
                message.lease_expires_at = self._parse_timestamp(
                    data["lease_expires_at"]
                )
            elif record.event_type == "message_acked":
                message = self._message_for_replay_locked(message_id)
                if message.state is not MessageState.IN_FLIGHT:
                    raise WALCorruptionError(
                        f"message {message_id} was ACKed without an active lease"
                    )
                message.state = MessageState.COMPLETED
                message.completed_at = self._parse_timestamp(data["completed_at"])
                self._clear_active_lease_locked(message)
            elif record.event_type == "message_nacked":
                message = self._message_for_replay_locked(message_id)
                if message.state is not MessageState.IN_FLIGHT:
                    raise WALCorruptionError(
                        f"message {message_id} was NACKed without an active lease"
                    )
                message.state = MessageState(data["state"])
                message.available_at = self._parse_timestamp(data["available_at"])
                self._clear_active_lease_locked(message)
            elif record.event_type == "lease_expired":
                message = self._message_for_replay_locked(message_id)
                if message.state is not MessageState.IN_FLIGHT:
                    raise WALCorruptionError(
                        f"message {message_id} expired without an active lease"
                    )
                if message.receipt_handle != data["receipt_handle"]:
                    raise WALCorruptionError(
                        f"message {message_id} expiration has a stale receipt handle"
                    )
                message.state = MessageState.READY
                message.available_at = self._parse_timestamp(data["available_at"])
                self._clear_active_lease_locked(message)
            else:
                raise WALCorruptionError(
                    f"unrecognized queue event: {record.event_type}"
                )
        except (KeyError, TypeError, ValueError) as exc:
            raise WALCorruptionError(
                f"invalid {record.event_type} event at record {record.record_number}"
            ) from exc

    def _message_for_replay_locked(self, message_id: str) -> Message:
        try:
            return self._messages[message_id]
        except KeyError as exc:
            raise WALCorruptionError(
                f"WAL references unknown message ID: {message_id}"
            ) from exc

    @staticmethod
    def _serialize_timestamp(value: datetime) -> str:
        return value.isoformat()

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("WAL timestamp must include a timezone")
        return parsed

    def _ordered_ready_messages_locked(self) -> list[Message]:
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

    def _get_message_locked(self, message_id: str) -> Message:
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
    def _clear_active_lease_locked(message: Message) -> None:
        message.leased_by = None
        message.receipt_handle = None
        message.lease_expires_at = None

    @staticmethod
    def _snapshot_locked(message: Message) -> Message:
        return copy.deepcopy(message)

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
