# Queuemaxxing

Queuemaxxing is a FastAPI service that will power the Artie QueueLab durable
queue demo. Step 1 provides the application scaffold, health endpoint, and a
minimal placeholder page.

## Concurrency boundary

Each `QueueEngine` protects its state with a reentrant lock. Many producer and
consumer threads—and concurrent HTTP clients served by one Python process—can
use the same engine safely. Public reads return deep snapshots so callers cannot
mutate stored messages or nested payloads without going through the engine.

Multiple independent server processes cannot share one in-memory engine. A
durable engine enforces this boundary by giving one process exclusive ownership
of its WAL for the engine's lifetime.

## Durability and recovery

`QueueEngine.open_durable()` stores configuration, enqueue, claim, ACK, NACK,
and lease-expiration events in a local append-only write-ahead log. Each event
is one versioned JSON line with a consecutive record number and a SHA-256
checksum over its canonical JSON body.

The engine appends each transition, flushes Python's file buffer, and calls
`fsync` before changing memory. A failed write therefore leaves the in-memory
transition unapplied, poisons the WAL against later writes, and returns an
error. On restart, records are checksum-validated and replayed in order to
restore messages, ordering configuration, sequences, delivery attempts, and
active leases. Expired leases are durably requeued during recovery; delayed
eligibility is derived from the stored availability timestamp.

A non-newline-terminated final record is treated as a crash-torn tail and
truncated before recovery. Malformed, unsupported, nonconsecutive, or
checksum-invalid complete records stop recovery as corruption. The WAL holds a
non-blocking OS file lock for its entire lifetime, so one server process owns a
queue data file at a time. WAL compaction, snapshots, and multi-process or
multi-node operation are future work.

## Requirements

- Python 3.11 or newer

## Set up

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

If your `python3` command already points to Python 3.11 or newer, you can use
`python3 -m venv .venv` instead.

## Run the server

```bash
.venv/bin/python run.py
```

Open <http://localhost:8000> for the placeholder interface or
<http://localhost:8000/health> for the health check.

## Run tests

```bash
.venv/bin/python -m pytest
```
