"""taskqueue: a small, honest background job queue. SQLite-backed, no
broker, no external dependencies -- built to demonstrate the core ideas
(atomic job claiming, retry with backoff, worker polling) that bigger
systems like Celery/RQ/Sidekiq are built on top of.
"""
