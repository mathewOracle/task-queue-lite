"""Worker: polls the queue for due jobs and executes them via the task
registry. One job at a time, single-threaded by design -- run multiple
worker processes for concurrency (they share the queue safely, see
`JobQueue.claim_next`).
"""
from __future__ import annotations

import time

from taskqueue.queue import JobQueue
from taskqueue.registry import get_task


class Worker:
    def __init__(self, queue: JobQueue, backoff_base: float = 1.0):
        self.queue = queue
        self.backoff_base = backoff_base

    def run_once(self) -> bool:
        """Claim and execute a single due job. Returns True if a job was
        found and processed (regardless of success/failure), False if the
        queue had nothing due right now.
        """
        job = self.queue.claim_next()
        if job is None:
            return False

        try:
            func = get_task(job.task_name)
            result = func(*job.args, **job.kwargs)
            self.queue.mark_done(job.id, result)
        except Exception as exc:  # noqa: BLE001 - a task can raise anything
            self.queue.mark_failed_or_retry(job.id, str(exc), self.backoff_base)
        return True

    def run_forever(self, poll_interval: float = 0.5, max_iterations: int | None = None) -> None:
        """Poll continuously. `max_iterations` exists purely so tests and
        demos can run this without needing Ctrl+C.
        """
        iterations = 0
        while max_iterations is None or iterations < max_iterations:
            processed = self.run_once()
            if not processed:
                time.sleep(poll_interval)
            iterations += 1
