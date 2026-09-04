"""Task registry: a decorator that maps a name -> callable, so jobs on the
queue can reference work by name+args instead of pickling functions
(safer, and works across process boundaries without import gymnastics --
the same pattern Celery/RQ use).
"""
from __future__ import annotations

from typing import Callable

_REGISTRY: dict[str, Callable] = {}


def task(name: str | None = None):
    """Decorator: `@task()` or `@task("custom_name")` registers a function
    so it can be looked up by name when a worker executes a queued job.
    """

    def decorator(func: Callable) -> Callable:
        key = name or func.__name__
        _REGISTRY[key] = func
        return func

    return decorator


def get_task(name: str) -> Callable:
    if name not in _REGISTRY:
        raise KeyError(f"No task registered under name '{name}'")
    return _REGISTRY[name]


def registered_tasks() -> list[str]:
    return sorted(_REGISTRY.keys())
