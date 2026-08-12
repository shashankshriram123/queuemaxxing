from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from app.models import QueueConfig, QueueOrder
from app.queue_engine import QueueEngine
from app.wal import (
    FORMAT_VERSION,
    WALCorruptionError,
    WALLockedError,
    WALWriteError,
    WriteAheadLog,
    calculate_checksum,
    canonical_json,
)


def read_raw_records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def rewrite_records(path: Path, records: list[dict]) -> None:
    path.write_bytes(b"".join(canonical_json(record) + b"\n" for record in records))


def test_new_durable_engine_creates_wal_and_configuration_record(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nested" / "queue.wal"

    engine = QueueEngine.open_durable(
        path, QueueConfig(order=QueueOrder.LIFO, priority_enabled=True)
    )
    engine.close()

    records = read_raw_records(path)
    assert path.is_file()
    assert len(records) == 1
    assert records[0]["event_type"] == "queue_configured"
    assert records[0]["data"] == {
        "order": "lifo",
        "priority_enabled": True,
    }


def test_append_writes_newline_checksum_and_consecutive_numbers(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    with WriteAheadLog(path) as wal:
        wal.append(
            "queue_configured", {"order": "fifo", "priority_enabled": False}
        )
        wal.append(
            "message_enqueued",
            {
                "message_id": "message-1",
                "payload": {"name": "job"},
                "priority": 3,
                "sequence": 1,
                "created_at": "2026-01-01T00:00:00+00:00",
                "available_at": "2026-01-01T00:00:00+00:00",
                "state": "ready",
            },
        )

    raw = path.read_bytes()
    records = read_raw_records(path)
    assert raw.endswith(b"\n")
    assert [record["record_number"] for record in records] == [1, 2]
    for record in records:
        body = {key: value for key, value in record.items() if key != "checksum"}
        assert record["version"] == FORMAT_VERSION
        assert record["checksum"] == calculate_checksum(body)


def test_every_successful_append_is_flushed_with_fsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []
    real_fsync = os.fsync

    def recording_fsync(file_descriptor: int) -> None:
        calls.append(file_descriptor)
        real_fsync(file_descriptor)

    monkeypatch.setattr("app.wal.os.fsync", recording_fsync)
    with WriteAheadLog(tmp_path / "queue.wal") as wal:
        wal.append(
            "queue_configured", {"order": "fifo", "priority_enabled": False}
        )

    assert len(calls) == 1


def test_non_newline_terminated_final_tail_is_ignored_and_truncated(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    with WriteAheadLog(path) as wal:
        wal.append(
            "queue_configured", {"order": "fifo", "priority_enabled": False}
        )
    valid_size = path.stat().st_size
    with path.open("ab") as file:
        file.write(b'{"version":1,"record_number":2')

    with WriteAheadLog(path) as recovered:
        assert len(recovered.records) == 1

    assert path.stat().st_size == valid_size
    assert path.read_bytes().endswith(b"\n")


def test_malformed_complete_record_in_middle_is_corruption(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    engine = QueueEngine.open_durable(path)
    engine.enqueue({"name": "job"})
    engine.close()
    lines = path.read_bytes().splitlines(keepends=True)
    path.write_bytes(lines[0] + b"{not-json}\n" + b"".join(lines[1:]))

    with pytest.raises(WALCorruptionError, match="malformed complete"):
        WriteAheadLog(path)


def test_checksum_mismatch_is_corruption(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    engine = QueueEngine.open_durable(path)
    engine.close()
    records = read_raw_records(path)
    records[0]["checksum"] = "0" * 64
    rewrite_records(path, records)

    with pytest.raises(WALCorruptionError, match="checksum mismatch"):
        WriteAheadLog(path)


def test_unsupported_format_version_is_corruption(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    engine = QueueEngine.open_durable(path)
    engine.close()
    records = read_raw_records(path)
    records[0]["version"] = FORMAT_VERSION + 1
    rewrite_records(path, records)

    with pytest.raises(WALCorruptionError, match="unsupported WAL format"):
        WriteAheadLog(path)


@pytest.mark.parametrize("record_number", [0, 2, 50])
def test_missing_or_nonconsecutive_record_numbers_are_corruption(
    tmp_path: Path, record_number: int
) -> None:
    path = tmp_path / f"queue-{record_number}.wal"
    engine = QueueEngine.open_durable(path)
    engine.close()
    records = read_raw_records(path)
    records[0]["record_number"] = record_number
    rewrite_records(path, records)

    with pytest.raises(WALCorruptionError, match="nonconsecutive"):
        WriteAheadLog(path)


def test_missing_record_field_is_corruption(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    engine = QueueEngine.open_durable(path)
    engine.close()
    records = read_raw_records(path)
    del records[0]["event_type"]
    rewrite_records(path, records)

    with pytest.raises(WALCorruptionError, match="missing or unknown fields"):
        WriteAheadLog(path)


def test_unrecognized_event_type_is_corruption_even_with_valid_checksum(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    engine = QueueEngine.open_durable(path)
    engine.close()
    records = read_raw_records(path)
    records[0]["event_type"] = "mystery_event"
    body = {key: value for key, value in records[0].items() if key != "checksum"}
    records[0]["checksum"] = calculate_checksum(body)
    rewrite_records(path, records)

    with pytest.raises(WALCorruptionError, match="unrecognized WAL event"):
        WriteAheadLog(path)


def test_invalid_event_data_is_corruption_even_with_valid_checksum(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.wal"
    engine = QueueEngine.open_durable(path)
    engine.close()
    records = read_raw_records(path)
    records[0]["data"]["order"] = "random"
    body = {key: value for key, value in records[0].items() if key != "checksum"}
    records[0]["checksum"] = calculate_checksum(body)
    rewrite_records(path, records)

    with pytest.raises(WALCorruptionError, match="invalid order"):
        WriteAheadLog(path)


def test_second_active_owner_cannot_lock_same_wal(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    first = QueueEngine.open_durable(path)
    try:
        with pytest.raises(WALLockedError):
            QueueEngine.open_durable(path)
    finally:
        first.close()


def test_closing_owner_releases_lock_for_recovery(tmp_path: Path) -> None:
    path = tmp_path / "queue.wal"
    first = QueueEngine.open_durable(path)
    message = first.enqueue({"name": "survives"})
    first.close()

    second = QueueEngine.open_durable(path)
    try:
        assert second.get_message(message.id).payload == {"name": "survives"}
    finally:
        second.close()


def test_append_failure_leaves_engine_memory_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wal = WriteAheadLog(tmp_path / "queue.wal")
    engine = QueueEngine(wal=wal)
    real_append = wal.append

    def fail_append(*_: object, **__: object) -> None:
        raise WALWriteError("simulated append failure")

    monkeypatch.setattr(wal, "append", fail_append)
    with pytest.raises(WALWriteError, match="simulated"):
        engine.enqueue({"name": "must-not-appear"})

    assert engine.messages() == []
    monkeypatch.setattr(wal, "append", real_append)
    assert engine.enqueue({"name": "next"}).sequence == 1
    engine.close()


def test_fsync_failure_does_not_apply_transition_and_wal_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "queue.wal"
    engine = QueueEngine.open_durable(path)
    real_fsync = os.fsync

    def fail_fsync(_: int) -> None:
        raise OSError("simulated disk failure")

    monkeypatch.setattr("app.wal.os.fsync", fail_fsync)
    with pytest.raises(WALWriteError, match="append or fsync"):
        engine.enqueue({"name": "must-not-appear"})
    assert engine.messages() == []

    with pytest.raises(WALWriteError, match="unhealthy"):
        engine.enqueue({"name": "also-rejected"})
    assert engine.messages() == []
    engine.close()

    monkeypatch.setattr("app.wal.os.fsync", real_fsync)
    recovered = QueueEngine.open_durable(path)
    try:
        assert recovered.messages() == []
    finally:
        recovered.close()


def test_ack_append_failure_preserves_active_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wal = WriteAheadLog(tmp_path / "queue.wal")
    engine = QueueEngine(wal=wal)
    message = engine.enqueue({"name": "leased"})
    lease = engine.receive("worker")
    assert lease is not None
    assert lease.receipt_handle is not None

    def fail_append(*_: object, **__: object) -> None:
        raise WALWriteError("simulated ACK append failure")

    monkeypatch.setattr(wal, "append", fail_append)
    with pytest.raises(WALWriteError, match="simulated ACK"):
        engine.ack(message.id, lease.receipt_handle)

    unchanged = engine.get_message(message.id)
    assert unchanged.state.value == "in_flight"
    assert unchanged.receipt_handle == lease.receipt_handle
    assert unchanged.leased_by == "worker"
    engine.close()


def test_clear_completed_append_failure_preserves_completed_messages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wal = WriteAheadLog(tmp_path / "queue.wal")
    engine = QueueEngine(wal=wal)
    message = engine.enqueue({"name": "done"})
    lease = engine.receive("worker")
    assert lease is not None
    assert lease.receipt_handle is not None
    engine.ack(message.id, lease.receipt_handle)

    def fail_append(*_: object, **__: object) -> None:
        raise WALWriteError("simulated clear append failure")

    monkeypatch.setattr(wal, "append", fail_append)
    with pytest.raises(WALWriteError, match="simulated clear"):
        engine.clear_completed()

    assert engine.get_message(message.id).state.value == "completed"
    engine.close()
