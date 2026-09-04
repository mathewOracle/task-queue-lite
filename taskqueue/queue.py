"""SQLite-backed job queue: enqueue, atomically claim, and finalize jobs.

The interesting bit is `claim_next`: multiple workers can poll the same
queue concurrently without ever double-processing a job. It uses an
optimistic-concurrency pattern (SELECT a candidate, then UPDATE it with a
WHERE clause that re-checks the status) rather than relying on
SQLite-version-specific UPDATE...RETURNING syntax, so it works everywhere.
"""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "queue.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT NOT NULL,
    args_json TEXT NOT NULL,
    kwargs_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    next_run_at REAL NOT NULL,
    result_json TEXT,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status_next_run
    ON jobs (status, next_run_at);
"""

PENDING, RUNNING, DONE, FAILED = "pending", "running", "done", "failed"


@dataclass(frozen=True)
class Job:
    id: int
    task_name: str
    args: list
    kwargs: dict
    status: str
    attempts: int
    max_attempts: int

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        return cls(
            id=row["id"],
            task_name=row["task_name"],
            args=json.loads(row["args_json"]),
            kwargs=json.loads(row["kwargs_json"]),
            status=row["status"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
        )


class JobQueue:
    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def enqueue(
        self,
        task_name: str,
        args: list | None = None,
        kwargs: dict | None = None,
        max_attempts: int = 3,
    ) -> int:
        now = time.time()
        cur = self._conn.execute(
            "INSERT INTO jobs (task_name, args_json, kwargs_json, status, "
            "attempts, max_attempts, next_run_at, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)",
            (
                task_name,
                json.dumps(args or []),
                json.dumps(kwargs or {}),
                PENDING,
                max_attempts,
                now,
                now,
                now,
            ),
        )
        self._conn.commit()
        return cur.lastrowid

    def claim_next(self) -> Job | None:
        """Atomically claim one due pending job, or None if there isn't one.

        Loops in case of a lost race with another worker (the UPDATE
        affects 0 rows if someone else claimed it first) -- with a small
        candidate pool this converges immediately in practice.
        """
        now = time.time()
        for _ in range(5):
            row = self._conn.execute(
                "SELECT id FROM jobs WHERE status = ? AND next_run_at <= ? "
                "ORDER BY id LIMIT 1",
                (PENDING, now),
            ).fetchone()
            if row is None:
                return None
            job_id = row["id"]
            cur = self._conn.execute(
                "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
                (RUNNING, now, job_id, PENDING),
            )
            self._conn.commit()
            if cur.rowcount == 1:
                claimed = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
                return Job.from_row(claimed)
        return None

    def mark_done(self, job_id: int, result: Any) -> None:
        self._conn.execute(
            "UPDATE jobs SET status = ?, result_json = ?, updated_at = ? WHERE id = ?",
            (DONE, json.dumps(result), time.time(), job_id),
        )
        self._conn.commit()

    def mark_failed_or_retry(self, job_id: int, error: str, backoff_base: float = 1.0) -> None:
        """Increment attempts. Reschedule with exponential backoff if
        attempts remain, otherwise mark permanently failed.
        """
        row = self._conn.execute("SELECT attempts, max_attempts FROM jobs WHERE id = ?", (job_id,)).fetchone()
        attempts = row["attempts"] + 1
        now = time.time()
        if attempts < row["max_attempts"]:
            delay = backoff_base * (2 ** attempts)
            self._conn.execute(
                "UPDATE jobs SET status = ?, attempts = ?, next_run_at = ?, "
                "error = ?, updated_at = ? WHERE id = ?",
                (PENDING, attempts, now + delay, error, now, job_id),
            )
        else:
            self._conn.execute(
                "UPDATE jobs SET status = ?, attempts = ?, error = ?, updated_at = ? WHERE id = ?",
                (FAILED, attempts, error, now, job_id),
            )
        self._conn.commit()

    def get(self, job_id: int) -> sqlite3.Row | None:
        return self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    def list_by_status(self, status: str, limit: int = 20) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM jobs WHERE status = ? ORDER BY id DESC LIMIT ?",
            (status, limit),
        ).fetchall()

    def close(self) -> None:
        self._conn.close()
