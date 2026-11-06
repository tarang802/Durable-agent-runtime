"""What a running activity can see and do: its own info, and heartbeats."""

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import asyncpg

from durably import store
from durably.errors import StaleWorkerError


@dataclass(frozen=True)
class ActivityInfo:
    workflow_id: str
    name: str
    idempotency_key: str
    attempt: int
    heartbeat_details: Any  # last details saved by a previous attempt, or None


@dataclass(frozen=True)
class _Running:
    info: ActivityInfo
    pool: asyncpg.Pool
    claim: store.Claim
    worker_id: str
    lease_s: float


_current: ContextVar[_Running] = ContextVar("durably_activity")


def _running() -> _Running:
    try:
        return _current.get()
    except LookupError:
        raise RuntimeError("only available inside a running activity") from None


def activity_info() -> ActivityInfo:
    return _running().info


async def heartbeat(details: Any = None) -> None:
    """Tell durably this activity is still alive, optionally saving progress.

    Extends the lease by the activity's heartbeat timeout. If the activity is retried,
    the next attempt sees the last `details` in `activity_info().heartbeat_details`.
    Raises StaleWorkerError if the task was already handed to another worker; stop work.
    """
    run = _running()
    async with run.pool.acquire() as conn:
        if not await store.heartbeat(conn, run.claim, run.worker_id, run.lease_s, details):
            raise StaleWorkerError("activity lease lost: another worker owns this task now")
