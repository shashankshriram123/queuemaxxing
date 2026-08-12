# Queuemaxxing

## Run it

Requires Python 3.11 or newer.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python run.py
```

Open <http://localhost:8000>. API docs are at <http://localhost:8000/docs>.

## What to look for

- Send one message or a concurrent burst.
- Try FIFO/LIFO, priority, and delayed messages.
- Start simulated workers and watch messages move through the pipeline.
- Run the guided scenarios, including lease expiration and redelivery.
- Restart the server and confirm the WAL restores queue state.

1. How do you handle replayed messages?
- the queue has at least once delivery where if a worker failes to ACK before the lease runs out the msg becomes ready to resent. Each message keeps track of its own delivery attempts and consumers use the msg id as a key to avoid processing duplicates.
2.  How would you refactor the queue into Pub/Sub?
- By adding topics and subscriptions. Producesr would publish msgs to a topic, and subscriptions would maintain state (delivery, ACK position). Each subscriber would have its own copy of the msg rather than a shared one.

3. Monitoring and alerting, Encryption (learned about a new algorithm that encrypts packets using the pattern a knight moves around the board in chess)
4. This implementation doesn't require any extra services works straight out the repo, its useful for local development. If you arn't running production workflows/loads you should use this
