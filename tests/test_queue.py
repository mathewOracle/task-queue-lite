"""Tests for the JobQueue: enqueue, atomic claim, retry/backoff, done."""
import tempfile
import time
from pathlib import Path

import pytest

from taskqueue.queue import DONE, FAILED, PENDING, RUNNING, JobQueue


@pytest.fixture
def queue():
    with tempfile.TemporaryDirectory() as tmp:
        q = JobQueue(db_path=Path(tmp) / "test.db")
        yield q
        q.close()


def test_enqueue_creates_pending_job(queue):
    job_id = queue.enqueue("add", args=[1, 2])
    row = queue.get(job_id)
    assert row["status"] == PENDING
    assert row["task_name"] == "add"


def test_claim_next_moves_job_to_running(queue):
    queue.enqueue("add", args=[1, 2])
    job = queue.claim_next()
    assert job is not None
    assert job.status == RUNNING
    assert job.args == [1, 2]


def test_claim_next_returns_none_when_empty(queue):
    assert queue.claim_next() is None


def test_two_claims_never_return_the_same_job(queue):
    queue.enqueue("add", args=[1, 2])
    first = queue.claim_next()
    second = queue.claim_next()
    assert first is not None
    assert second is None  # only one job existed


def test_mark_done_records_result(queue):
    job_id = queue.enqueue("add", args=[1, 2])
    queue.claim_next()
    queue.mark_done(job_id, 3)
    row = queue.get(job_id)
    assert row["status"] == DONE
    assert row["result_json"] == "3"


def test_failed_job_retries_until_max_attempts(queue):
    job_id = queue.enqueue("flaky_task", max_attempts=2)
    queue.claim_next()
    queue.mark_failed_or_retry(job_id, "boom", backoff_base=0.001)
    row = queue.get(job_id)
    assert row["status"] == PENDING  # one retry remaining
    assert row["attempts"] == 1

    # Wait out the tiny backoff, then fail again -- should now be terminal.
    time.sleep(0.05)
    queue.claim_next()
    queue.mark_failed_or_retry(job_id, "boom again", backoff_base=0.001)
    row = queue.get(job_id)
    assert row["status"] == FAILED
    assert row["attempts"] == 2


def test_list_by_status_filters_correctly(queue):
    queue.enqueue("add", args=[1, 1])
    queue.enqueue("add", args=[2, 2])
    pending = queue.list_by_status(PENDING)
    assert len(pending) == 2
