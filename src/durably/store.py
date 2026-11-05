"""Claim and append: the operations every durability guarantee rests on.

All lease times use the database clock (`now()`), never a worker's clock. Callers that
combine several of these functions must wrap them in one transaction.
"""

from dataclasses import dataclass
from uuid import UUID

import asyncpg

from durably.errors import StaleWorkerError


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
    """Rule 1: claim a due workflow task and take the workflow's lease, in one transaction.

    A task is due if it is unclaimed or its lease expired (a crashed worker's task is
    reclaimed here; ADR 0005). The workflow must not be leased by a live worker, so one
    worker replays a workflow at a time. Bumping `epoch` fences out whoever held it before.
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
    """Claim a due activity task. Does not touch the workflow's epoch (ADR 0006)."""
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
                         lease_until = now() + make_interval(secs => $2),
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
    """Rule 2: append one event, fenced by epoch. Returns the event's seq.

    The epoch check is part of the write itself, not a read before it: a worker that
    paused after any earlier check still matches zero rows here (G2). Taking the next
    seq from `workflows.next_seq` under its row lock keeps seqs gap-free and unique (G1).
    """
    seq = await conn.fetchval(
        "UPDATE workflows SET next_seq = next_seq + 1 WHERE id = $1 AND epoch = $2 RETURNING next_seq - 1",
        workflow_id, epoch,
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
        "UPDATE workflows SET lease_owner = NULL, lease_until = NULL WHERE id = $1 AND epoch = $2",
        claim.workflow_id, claim.epoch,
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
    """Rule 4: record an activity's outcome only if this claim still owns the task.

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
        # Not epoch-fenced: the activity worker never holds the workflow's epoch. The
        # next_seq row lock still serializes this with any concurrent workflow append.
        seq = await conn.fetchval(
            """
            UPDATE workflows SET next_seq = next_seq + 1
            WHERE id = $1 AND status = 'running' RETURNING next_seq - 1
            """,
            claim.workflow_id,
        )
        if seq is None:  # workflow already finished; nothing is waiting for this result
            return True
        await conn.execute(
            "INSERT INTO events (workflow_id, seq, type, payload) VALUES ($1, $2, $3, $4)",
            claim.workflow_id, seq, type, payload,
        )
        await wake_workflow(conn, claim.workflow_id)
    return True


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
