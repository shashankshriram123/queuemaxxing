# Queuemaxxing

Queuemaxxing is a FastAPI service that will power the Artie QueueLab durable
queue demo. Step 1 provides the application scaffold, health endpoint, and a
minimal placeholder page.

## Concurrency boundary

Each `QueueEngine` protects its state with a reentrant lock. Many producer and
consumer threads—and concurrent HTTP clients served by one Python process—can
use the same engine safely. Public reads return deep snapshots so callers cannot
mutate stored messages or nested payloads without going through the engine.

Multiple independent server processes sharing an in-memory engine or the same
data directory are not supported. A later persistence phase will add exclusive
data-directory ownership for that boundary.

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
