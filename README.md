# task-queue-lite

A small, honest background job queue -- SQLite-backed, zero external
brokers (no Redis, no RabbitMQ), demonstrating the core mechanics that
Celery/RQ/Sidekiq are built on: a task registry, atomic job claiming
(safe for multiple concurrent workers), and retry with exponential
backoff.

## Why this exists

Not every project needs a full message broker to get "run this later,
retry it if it fails" behavior. This is the minimal version of that --
and a good way to actually understand what a job queue is doing under
the hood before reaching for Celery in a bigger system.

## Install

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Usage

```bash
# See what's registered (add, slow_echo, flaky_task are demo tasks)
python -m taskqueue.cli tasks

# Enqueue a job
python -m taskqueue.cli enqueue add --args '[2, 3]'

# Run a worker (Ctrl+C to stop, or --max-iterations for a demo run)
python -m taskqueue.cli work --max-iterations 20

# Check on things
python -m taskqueue.cli status --status done
python -m taskqueue.cli status --status failed
```

Try the retry/backoff behavior directly:

```bash
python -m taskqueue.cli enqueue flaky_task --kwargs '{"fail_probability": 0.9}' --max-attempts 4
python -m taskqueue.cli work --max-iterations 20
python -m taskqueue.cli status --status failed
```

## Project layout

```
taskqueue/
  registry.py   # @task decorator: name -> callable lookup
  tasks.py       # example registered tasks (add, slow_echo, flaky_task)
  queue.py        # SQLite persistence + atomic claim logic
  worker.py        # polls the queue, executes tasks, handles retry
  cli.py             # enqueue / work / status commands
tests/
  test_queue.py
  test_worker.py
```

## Design notes

- **Atomic claiming without SQLite-version-specific syntax** -- `claim_next`
  does SELECT-then-UPDATE-with-a-status-guard rather than relying on
  `UPDATE ... RETURNING` (only available in SQLite 3.35+). If the UPDATE
  affects 0 rows, another worker won the race and this one retries against
  the next candidate. Multiple worker processes can safely share one
  queue file.
- **Named tasks, not pickled functions** -- jobs store a task *name* plus
  JSON-serializable args/kwargs, looked up in a registry at execution
  time. This is deliberately how Celery does it too: it avoids the
  security and portability problems of pickling arbitrary callables.
- **Exponential backoff on retry** -- `delay = base * 2^attempts`, capped
  by `max_attempts`. After that, a job is marked permanently `failed`
  rather than retried forever.

## Testing

```bash
uv pip install -r requirements-dev.txt
pytest
```

## License

MIT.
