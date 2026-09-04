"""CLI for enqueuing jobs, running a worker, and inspecting status."""
from __future__ import annotations

import json

import click
from rich.console import Console
from rich.table import Table

import taskqueue.tasks  # noqa: F401 - registers the demo tasks
from taskqueue.queue import DONE, FAILED, PENDING, RUNNING, JobQueue
from taskqueue.registry import registered_tasks
from taskqueue.worker import Worker

console = Console()


@click.group()
def cli():
    """taskqueue: a small, honest background job queue."""


@cli.command()
def tasks():
    """List all registered task names."""
    for name in registered_tasks():
        console.print(f"- {name}")


@cli.command()
@click.argument("task_name")
@click.option("--args", default="[]", help="JSON list of positional args.")
@click.option("--kwargs", default="{}", help="JSON object of keyword args.")
@click.option("--max-attempts", default=3, help="Retry limit before permanent failure.")
def enqueue(task_name: str, args: str, kwargs: str, max_attempts: int):
    """Enqueue a job by task name, e.g.:

    taskqueue enqueue add --args '[2, 3]'
    """
    queue = JobQueue()
    job_id = queue.enqueue(task_name, json.loads(args), json.loads(kwargs), max_attempts)
    queue.close()
    console.print(f"[green]Enqueued job {job_id}[/green] ({task_name})")


@cli.command()
@click.option("--poll-interval", default=0.5, help="Seconds to wait when the queue is empty.")
@click.option("--max-iterations", default=None, type=int, help="Stop after N loop iterations (default: run forever).")
def work(poll_interval: float, max_iterations: int | None):
    """Run a worker loop, processing jobs as they become due."""
    queue = JobQueue()
    worker = Worker(queue)
    console.print("[cyan]Worker started. Ctrl+C to stop.[/cyan]")
    try:
        worker.run_forever(poll_interval=poll_interval, max_iterations=max_iterations)
    except KeyboardInterrupt:
        console.print("[yellow]Worker stopped.[/yellow]")
    finally:
        queue.close()


@cli.command()
@click.option("--status", type=click.Choice([PENDING, RUNNING, DONE, FAILED]), default=PENDING)
@click.option("--limit", default=20)
def status(status: str, limit: int):
    """List recent jobs in a given status."""
    queue = JobQueue()
    rows = queue.list_by_status(status, limit)
    queue.close()

    table = Table(title=f"Jobs: {status}")
    table.add_column("ID")
    table.add_column("Task")
    table.add_column("Attempts")
    table.add_column("Result/Error")
    for row in rows:
        detail = row["result_json"] or row["error"] or ""
        table.add_row(str(row["id"]), row["task_name"], str(row["attempts"]), str(detail))
    console.print(table)


if __name__ == "__main__":
    cli()
