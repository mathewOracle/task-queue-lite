"""Example tasks. In a real project these would live wherever your
business logic does -- the queue only needs the name to be registered
before a worker tries to run it.
"""
from __future__ import annotations

import random
import time

from taskqueue.registry import task


@task()
def add(a: int, b: int) -> int:
    return a + b


@task()
def slow_echo(message: str, seconds: float = 0.1) -> str:
    time.sleep(seconds)
    return message


@task()
def flaky_task(fail_probability: float = 0.5) -> str:
    """Fails randomly -- useful for demonstrating retry/backoff behavior."""
    if random.random() < fail_probability:
        raise RuntimeError("Simulated transient failure")
    return "succeeded"
