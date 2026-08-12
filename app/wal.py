"""Durable, checksummed, single-owner write-ahead log storage."""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import math
import os
import threading
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Final


FORMAT_VERSION: Final = 1
EVENT_TYPES: Final = frozenset(
    {
        "queue_configured",
        "queue_config_updated",
        "message_enqueued",
        "message_claimed",
        "message_acked",
        "message_nacked",
        "lease_expired",
    }
)
EVENT_FIELDS: Final = {
    "queue_configured": {"order", "priority_enabled"},
    "queue_config_updated": {"order", "priority_enabled"},
    "message_enqueued": {
        "message_id",
        "payload",
        "priority",
        "sequence",
        "created_at",
        "available_at",
        "state",
    },
    "message_claimed": {
        "message_id",
        "worker_id",
        "receipt_handle",
        "lease_expires_at",
        "delivery_attempts",
    },
    "message_acked": {"message_id", "completed_at"},
    "message_nacked": {"message_id", "state", "available_at"},
    "lease_expired": {"message_id", "receipt_handle", "available_at"},
}


class WALError(Exception):
    """Base class for WAL storage failures."""


class WALCorruptionError(WALError):
    """Raised when complete durable history cannot be validated safely."""


class WALLockedError(WALError):
    """Raised when another process already owns the WAL."""


class WALWriteError(WALError):
    """Raised after an append or durability operation fails."""


@dataclass(frozen=True, slots=True)
class WALRecord:
    version: int
    record_number: int
    event_type: str
    data: dict[str, Any]
    checksum: str


def canonical_json(value: object) -> bytes:
    """Encode a value deterministically for hashing and storage."""

    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def calculate_checksum(body: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(body)).hexdigest()


def _parse_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise WALCorruptionError(f"event field '{field}' must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise WALCorruptionError(
            f"event field '{field}' must be an ISO timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise WALCorruptionError(f"event field '{field}' must include a timezone")
    return parsed


def _require_string(data: dict[str, Any], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value:
        raise WALCorruptionError(f"event field '{field}' must be a non-empty string")
    return value


def _require_integer(
    data: dict[str, Any], field: str, *, minimum: int | None = 0
) -> int:
    value = data.get(field)
    if type(value) is not int or (minimum is not None and value < minimum):
        constraint = "an integer" if minimum is None else f"an integer >= {minimum}"
        raise WALCorruptionError(
            f"event field '{field}' must be {constraint}"
        )
    return value


def _validate_json_value(value: object, field: str = "payload") -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise WALCorruptionError(f"event field '{field}' must contain finite JSON")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{field}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise WALCorruptionError(f"event field '{field}' has a non-string key")
            _validate_json_value(item, f"{field}.{key}")
        return
    raise WALCorruptionError(f"event field '{field}' is not JSON-compatible")


def validate_event_data(event_type: str, data: object) -> dict[str, Any]:
    """Validate the durable shape of a recognized event."""

    if event_type not in EVENT_TYPES:
        raise WALCorruptionError(f"unrecognized WAL event type: {event_type!r}")
    if not isinstance(data, dict):
        raise WALCorruptionError("WAL event data must be an object")
    if set(data) != EVENT_FIELDS[event_type]:
        raise WALCorruptionError(
            f"{event_type} event has missing or unknown data fields"
        )

    if event_type in {"queue_configured", "queue_config_updated"}:
        if data.get("order") not in {"fifo", "lifo"}:
            raise WALCorruptionError("queue configuration has an invalid order")
        if type(data.get("priority_enabled")) is not bool:
            raise WALCorruptionError(
                "queue configuration priority_enabled must be a boolean"
            )
    elif event_type == "message_enqueued":
        _require_string(data, "message_id")
        _validate_json_value(data.get("payload"))
        _require_integer(data, "priority", minimum=None)
        _require_integer(data, "sequence", minimum=1)
        _parse_timestamp(data.get("created_at"), "created_at")
        _parse_timestamp(data.get("available_at"), "available_at")
        if data.get("state") not in {"ready", "delayed"}:
            raise WALCorruptionError("enqueued message has an invalid initial state")
    elif event_type == "message_claimed":
        _require_string(data, "message_id")
        _require_string(data, "worker_id")
        _require_string(data, "receipt_handle")
        _parse_timestamp(data.get("lease_expires_at"), "lease_expires_at")
        _require_integer(data, "delivery_attempts", minimum=1)
    elif event_type == "message_acked":
        _require_string(data, "message_id")
        _parse_timestamp(data.get("completed_at"), "completed_at")
    elif event_type == "message_nacked":
        _require_string(data, "message_id")
        if data.get("state") not in {"ready", "delayed"}:
            raise WALCorruptionError("NACK event has an invalid state")
        _parse_timestamp(data.get("available_at"), "available_at")
    elif event_type == "lease_expired":
        _require_string(data, "message_id")
        _require_string(data, "receipt_handle")
        _parse_timestamp(data.get("available_at"), "available_at")

    return data


class WriteAheadLog:
    """Append-only JSONL WAL held under a lifetime POSIX file lock."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise WALWriteError(
                f"cannot create WAL directory at {self.path.parent}"
            ) from exc
        self._thread_lock = threading.RLock()
        self._healthy = True
        self._closed = False
        self._records: list[WALRecord] = []

        try:
            self._file: BinaryIO = self.path.open("a+b")
        except OSError as exc:
            raise WALWriteError(f"cannot open WAL at {self.path}") from exc

        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._file.close()
            self._closed = True
            raise WALLockedError(f"WAL is already owned: {self.path}") from exc
        except OSError as exc:
            self._file.close()
            self._closed = True
            raise WALWriteError(f"cannot lock WAL at {self.path}") from exc

        try:
            self._records = self._read_and_validate_locked()
        except Exception:
            self.close()
            raise

    @property
    def records(self) -> tuple[WALRecord, ...]:
        with self._thread_lock:
            return tuple(deepcopy(self._records))

    @property
    def healthy(self) -> bool:
        with self._thread_lock:
            return self._healthy and not self._closed

    def append(self, event_type: str, data: dict[str, Any]) -> WALRecord:
        """Append, flush, and fsync one event before returning it."""

        with self._thread_lock:
            if self._closed:
                raise WALWriteError("WAL is closed")
            if not self._healthy:
                raise WALWriteError("WAL is unhealthy after an earlier write failure")

            try:
                validated_data = json.loads(canonical_json(data))
                validate_event_data(event_type, validated_data)
                body = {
                    "version": FORMAT_VERSION,
                    "record_number": len(self._records) + 1,
                    "event_type": event_type,
                    "data": validated_data,
                }
                record_dict = {**body, "checksum": calculate_checksum(body)}
                encoded = canonical_json(record_dict) + b"\n"
            except (TypeError, ValueError, WALCorruptionError) as exc:
                raise WALWriteError("refusing to append an invalid WAL event") from exc

            start_offset = 0
            try:
                self._file.seek(0, os.SEEK_END)
                start_offset = self._file.tell()
                written = self._file.write(encoded)
                if written != len(encoded):
                    raise OSError("short WAL write")
                self._file.flush()
                os.fsync(self._file.fileno())
            except Exception as exc:
                self._healthy = False
                try:
                    self._file.seek(start_offset)
                    self._file.truncate()
                    self._file.flush()
                except Exception:
                    pass
                raise WALWriteError("WAL append or fsync failed") from exc

            record = WALRecord(**record_dict)
            self._records.append(record)
            return deepcopy(record)

    def close(self) -> None:
        with self._thread_lock:
            if self._closed:
                return
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            finally:
                self._file.close()
                self._closed = True

    def _read_and_validate_locked(self) -> list[WALRecord]:
        self._file.flush()
        self._file.seek(0)
        raw = self._file.read()

        if raw and not raw.endswith(b"\n"):
            last_newline = raw.rfind(b"\n")
            complete_length = last_newline + 1
            raw = raw[:complete_length]
            try:
                self._file.seek(complete_length)
                self._file.truncate()
                self._file.flush()
                os.fsync(self._file.fileno())
            except OSError as exc:
                self._healthy = False
                raise WALWriteError("cannot discard incomplete WAL tail") from exc

        records: list[WALRecord] = []
        for expected_number, line in enumerate(raw.splitlines(), start=1):
            records.append(self._decode_record(line, expected_number))

        self._file.seek(0, os.SEEK_END)
        return records

    @staticmethod
    def _decode_record(line: bytes, expected_number: int) -> WALRecord:
        try:
            decoded = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WALCorruptionError(
                f"malformed complete WAL record {expected_number}"
            ) from exc

        if not isinstance(decoded, dict):
            raise WALCorruptionError(f"WAL record {expected_number} must be an object")
        required = {"version", "record_number", "event_type", "data", "checksum"}
        if set(decoded) != required:
            raise WALCorruptionError(
                f"WAL record {expected_number} has missing or unknown fields"
            )
        if type(decoded["version"]) is not int or decoded["version"] != FORMAT_VERSION:
            raise WALCorruptionError(
                f"unsupported WAL format version in record {expected_number}"
            )
        if (
            type(decoded["record_number"]) is not int
            or decoded["record_number"] != expected_number
        ):
            raise WALCorruptionError(
                f"nonconsecutive WAL record number at record {expected_number}"
            )
        if not isinstance(decoded["event_type"], str):
            raise WALCorruptionError(
                f"WAL record {expected_number} has an invalid event type"
            )
        if not isinstance(decoded["checksum"], str):
            raise WALCorruptionError(
                f"WAL record {expected_number} has an invalid checksum"
            )

        body = {
            "version": decoded["version"],
            "record_number": decoded["record_number"],
            "event_type": decoded["event_type"],
            "data": decoded["data"],
        }
        if not hmac.compare_digest(decoded["checksum"], calculate_checksum(body)):
            raise WALCorruptionError(
                f"checksum mismatch in WAL record {expected_number}"
            )
        validate_event_data(decoded["event_type"], decoded["data"])
        return WALRecord(**decoded)

    def __enter__(self) -> "WriteAheadLog":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
