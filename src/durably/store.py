"""Claim and append: the operations every durability guarantee rests on.

All lease times use the database clock (`now()`), never a worker's clock. Callers that
combine several of these functions must wrap them in one transaction.
"""

import os
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from durably.errors import StaleWorkerError
from durably.failpoints import failpoint


def fencing_enabled() -> bool:
    """`DURABLY_FENCING=off` drops the epoch condition from history writes.

    For tests only: it exists so a test can show the corruption fencing prevents.
    """
    return os.environ.get("DURABLY_FENCING", "on") != "off"


@dataclass(frozen=True)
class Claim:
    task_id: int
    workflow_id: UUID
    attempt: int
    payload: dict
    epoch: int | None = None  # fencing token; set for workflow tasks only


@dataclass(frozen=True)
class Event:
    seq: int
    type: str
    payload: dict


async def claim_workflow_task(conn: asyncpg.Connection, worker_id: str, lease_s: float) -> Claim | None:
    """Claim a due workflow task and take the workflow's lease, in one transaction.

    A task is due if it is unclaimed or its lease expired, so a crashed worker's task is
    reclaimed here without a separate reaper. The workflow must not be leased by a live
    worker, so one worker replays a workflow at a time. Bumping `epoch` fences out whoever
    held it before.
    """
    async with conn.transaction():
        row = await conn.fetchrow(
            """
            WITH due AS (
                SELECT t.id FROM tasks t JOIN workflows w ON w.id = t.workflow_id
                WHERE t.kind = 'workflow_task' AND t.visible_at <= now()
                  AND (t.claimed_by IS NULL OR t.lease_until < now())
                  AND (w.lease_until IS NULL OR w.lease_until < now())
                ORDER BY t.visible_at
                LIMIT 1
                FOR UPDATE OF t SKIP LOCKED
            )
            UPDATE tasks SET claimed_by = $1,
                             lease_until = now() + make_interval(secs => $2),
                             attempt = attempt + 1
            FROM due WHERE tasks.id = due.id
            RETURNING tasks.id, tasks.workflow_id, tasks.attempt, tasks.payload
            """,
            worker_id, lease_s,
        )
        if row is None:
            return None
        epoch = await conn.fetchval(
            """
            UPDATE workflows SET epoch = epoch + 1, lease_owner = $2,
                                 lease_until = now() + make_interval(secs => $3)
            WHERE id = $1 RETURNING epoch
            """,
            row["workflow_id"], worker_id, lease_s,
        )
    return Claim(row["id"], row["workflow_id"], row["attempt"], row["payload"], epoch)


async def claim_activity_task(conn: asyncpg.Connection, worker_id: str, lease_s: float) -> Claim | None:
    """Claim a due activity task.

    Does not touch the workflow's epoch: that would fence out the workflow worker. Activity
    results are guarded by the task's attempt instead (see `record_activity_result`).

    An activity with a heartbeat timeout is leased for that long instead, so a hung
    activity that stops heartbeating is reclaimed quickly.
    """
    row = await conn.fetchrow(
        """
        WITH due AS (
            SELECT id FROM tasks
            WHERE kind = 'activity_task' AND visible_at <= now()
              AND (claimed_by IS NULL OR lease_until < now())
            ORDER BY visible_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE tasks SET claimed_by = $1,
                         lease_until = now() + make_interval(
                             secs => COALESCE((tasks.payload->>'heartbeat_timeout')::float8, $2)),
                         attempt = attempt + 1
        FROM due WHERE tasks.id = due.id
        RETURNING tasks.id, tasks.workflow_id, tasks.attempt, tasks.payload
        """,
        worker_id, lease_s,
    )
    if row is None:
        return None
    return Claim(row["id"], row["workflow_id"], row["attempt"], row["payload"])


async def load_history(conn: asyncpg.Connection, workflow_id: UUID) -> list[Event]:
    rows = await conn.fetch(
        "SELECT seq, type, payload FROM events WHERE workflow_id = $1 ORDER BY seq", workflow_id
    )
    return [Event(r["seq"], r["type"], r["payload"]) for r in rows]


async def append(conn: asyncpg.Connection, workflow_id: UUID, epoch: int, type: str, payload: dict) -> int:
    """Append one event, fenced by epoch. Returns the event's seq.

    The epoch check is part of the write itself, not a read before it: a worker that
    paused after any earlier check still matches zero rows here (G2). Taking the next
    seq from `workflows.next_seq` under its row lock keeps seqs gap-free and unique (G1).
    """
    if fencing_enabled():
        seq = await conn.fetchval(
            "UPDATE workflows SET next_seq = next_seq + 1 WHERE id = $1 AND epoch = $2 RETURNING next_seq - 1",
            workflow_id, epoch,
        )
    else:
        seq = await conn.fetchval(
            "UPDATE workflows SET next_seq = next_seq + 1 WHERE id = $1 RETURNING next_seq - 1", workflow_id
        )
    if seq is None:
        raise StaleWorkerError(f"workflow {workflow_id}: epoch {epoch} is stale")
    await conn.execute(
        "INSERT INTO events (workflow_id, seq, type, payload) VALUES ($1, $2, $3, $4)",
        workflow_id, seq, type, payload,
    )
    return seq


async def wake_workflow(conn: asyncpg.Connection, workflow_id: UUID, delay_s: float = 0.0) -> None:
    """Make sure a workflow task is pending. Coalesces with one already pending."""
    await conn.execute(
        """
        INSERT INTO tasks (workflow_id, kind, visible_at)
        VALUES ($1, 'workflow_task', now() + make_interval(secs => $2))
        ON CONFLICT (workflow_id) WHERE kind = 'workflow_task' AND claimed_by IS NULL DO NOTHING
        """,
        workflow_id, delay_s,
    )


async def enqueue_activity(conn: asyncpg.Connection, workflow_id: UUID, payload: dict) -> None:
    await conn.execute(
        "INSERT INTO tasks (workflow_id, kind, payload) VALUES ($1, 'activity_task', $2)",
        workflow_id, payload,
    )


async def release_workflow(conn: asyncpg.Connection, claim: Claim, worker_id: str) -> None:
    """Finish a workflow task: drop it and give up the lease. Fenced like an append."""
    status = await conn.execute(
        "UPDATE workflows SET lease_owner = NULL, lease_until = NULL WHERE id = $1 AND (epoch = $2 OR NOT $3)",
        claim.workflow_id, claim.epoch, fencing_enabled(),
    )
    if status == "UPDATE 0":
        raise StaleWorkerError(f"workflow {claim.workflow_id}: epoch {claim.epoch} is stale")
    await conn.execute(
        "DELETE FROM tasks WHERE id = $1 AND claimed_by = $2 AND attempt = $3",
        claim.task_id, worker_id, claim.attempt,
    )


async def record_activity_result(
    conn: asyncpg.Connection, claim: Claim, worker_id: str, type: str, payload: dict
) -> bool:
    """Record an activity's outcome only if this claim still owns the task.

    Deleting the task with `attempt` and `claimed_by` in the WHERE clause is the guard: if
    the lease expired and another worker re-claimed the task, attempt moved on and this
    delete matches nothing, so the stale result is dropped. Returns False in that case.
    """
    async with conn.transaction():
        owned = await conn.fetchval(
            "DELETE FROM tasks WHERE id = $1 AND attempt = $2 AND claimed_by = $3 RETURNING id",
            claim.task_id, claim.attempt, worker_id,
        )
        if owned is None:
            return False
        await deliver(conn, claim.workflow_id, type, payload)
    return True


async def deliver(conn: asyncpg.Connection, workflow_id: UUID, type: str, payload: dict) -> int | None:
    """Append an event that arrives from outside the workflow task, and wake the workflow.

    Used for activity results, fired timers and signals. Not epoch-fenced: the writer
    never holds the workflow's epoch, and whatever guards the caller has (task attempt,
    timer row) decides whether to deliver. The `next_seq` row lock still serializes this
    with any concurrent workflow append. Returns None if the workflow is no longer running.
    """
    seq = await conn.fetchval(
        "UPDATE workflows SET next_seq = next_seq + 1 WHERE id = $1 AND status = 'running' RETURNING next_seq - 1",
        workflow_id,
    )
    if seq is None:
        return None
    await conn.execute(
        "INSERT INTO events (workflow_id, seq, type, payload) VALUES ($1, $2, $3, $4)",
        workflow_id, seq, type, payload,
    )
    await wake_workflow(conn, workflow_id)
    return seq


async def enqueue_timer(conn: asyncpg.Connection, workflow_id: UUID, started_seq: int, seconds: float) -> None:
    """Schedule a timer task; `visible_at` on the database clock is when it fires."""
    await conn.execute(
        """
        INSERT INTO tasks (workflow_id, kind, visible_at, payload)
        VALUES ($1, 'timer', now() + make_interval(secs => $2), $3)
        """,
        workflow_id, seconds, {"started_seq": started_seq},
    )


async def fire_due_timer(conn: asyncpg.Connection) -> bool:
    """Fire one due timer. Returns False if none is due.

    Deleting the timer task and appending TimerFired happen in one transaction, so a timer
    fires exactly once (G7): a worker that dies mid-way rolls back, and the timer stays for
    the next worker; two workers cannot both take it (SKIP LOCKED).
    """
    async with conn.transaction():
        timer = await conn.fetchrow(
            """
            DELETE FROM tasks WHERE id = (
                SELECT id FROM tasks WHERE kind = 'timer' AND visible_at <= now()
                ORDER BY visible_at LIMIT 1 FOR UPDATE SKIP LOCKED
            ) RETURNING workflow_id, payload
            """
        )
        if timer is None:
            return False
        failpoint("during_timer_fire")
        await deliver(conn, timer["workflow_id"], "TimerFired", {"started_seq": timer["payload"]["started_seq"]})
    return True


async def heartbeat(
    conn: asyncpg.Connection, claim: Claim, worker_id: str, lease_s: float, details=None
) -> bool:
    """Extend an activity's lease and save progress details for a later attempt.

    Guarded like a result: returns False if another worker has re-claimed the task.
    """
    status = await conn.execute(
        """
        UPDATE tasks SET lease_until = now() + make_interval(secs => $4),
                         payload = CASE WHEN $5::jsonb IS NULL THEN payload
                                        ELSE jsonb_set(payload, '{heartbeat_details}', $5) END
        WHERE id = $1 AND attempt = $2 AND claimed_by = $3
        """,
        claim.task_id, claim.attempt, worker_id, lease_s, details,
    )
    return status != "UPDATE 0"


async def retry_activity(conn: asyncpg.Connection, claim: Claim, worker_id: str, delay_s: float) -> bool:
    """Put a failed activity back in the queue after `delay_s`, if this claim still owns it."""
    status = await conn.execute(
        """
        UPDATE tasks SET claimed_by = NULL, lease_until = NULL,
                         visible_at = now() + make_interval(secs => $4)
        WHERE id = $1 AND attempt = $2 AND claimed_by = $3
        """,
        claim.task_id, claim.attempt, worker_id, delay_s,
    )
    return status != "UPDATE 0"
