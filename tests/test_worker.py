"""Tests for the Worker: executing registered tasks off the queue."""
import tempfile
from pathlib import Path

import pytest

import taskqueue.tasks  # noqa: F401 - registers add/slow_echo/flaky_task
from taskqueue.queue import DONE, FAILED, JobQueue
from taskqueue.worker import Worker


@pytest.fixture
def queue():
    with tempfile.TemporaryDirectory() as tmp:
        q = JobQueue(db_path=Path(tmp) / "test.db")
        yield q
        q.close()


def test_run_once_executes_successful_task(queue):
    job_id = queue.enqueue("add", args=[2, 3])
    worker = Worker(queue)
    processed = worker.run_once()
    assert processed is True
    row = queue.get(job_id)
    assert row["status"] == DONE
    assert row["result_json"] == "5"


def test_run_once_returns_false_on_empty_queue(queue):
    worker = Worker(queue)
    assert worker.run_once() is False


def test_run_once_marks_permanent_failure_after_max_attempts(queue):
    job_id = queue.enqueue("flaky_task", kwargs={"fail_probability": 1.0}, max_attempts=1)
    worker = Worker(queue, backoff_base=0.001)
    worker.run_once()
    row = queue.get(job_id)
    assert row["status"] == FAILED
    assert "Simulated transient failure" in row["error"]


def test_run_forever_processes_all_queued_jobs(queue):
    for i in range(3):
        queue.enqueue("add", args=[i, i])
    worker = Worker(queue)
    worker.run_forever(poll_interval=0.01, max_iterations=5)
    done_jobs = queue.list_by_status(DONE)
    assert len(done_jobs) == 3


def test_unknown_task_name_fails_gracefully(queue):
    job_id = queue.enqueue("nonexistent_task", max_attempts=1)
    worker = Worker(queue, backoff_base=0.001)
    worker.run_once()
    row = queue.get(job_id)
    assert row["status"] == FAILED
    assert "No task registered" in row["error"]
