"""The claim/append rules, exercised directly against the database."""

import asyncio

import asyncpg
import pytest

from durably import StaleWorkerError, store


async def new_workflow(db) -> str:
    wf = await db.fetchval("INSERT INTO workflows (type) VALUES ('t') RETURNING id")
    await store.wake_workflow(db, wf)
    return wf


async def test_claim_bumps_epoch(pool, db):
    wf = await new_workflow(db)
    async with pool.acquire() as conn:
        claim = await store.claim_workflow_task(conn, "w1", 5.0)
    assert claim.workflow_id == wf
    assert claim.epoch == 1
    row = await db.fetchrow("SELECT epoch, lease_owner FROM workflows WHERE id = $1", wf)
    assert (row["epoch"], row["lease_owner"]) == (1, "w1")


async def test_append_with_stale_epoch_is_rejected(pool, db):
    wf = await new_workflow(db)
    async with pool.acquire() as conn:
        claim = await store.claim_workflow_task(conn, "w1", 5.0)
        # Someone else takes over: the epoch moves on.
        await conn.execute("UPDATE workflows SET epoch = epoch + 1 WHERE id = $1", wf)
        with pytest.raises(StaleWorkerError):
            async with conn.transaction():
                await store.append(conn, wf, claim.epoch, "MarkerRecorded", {})
    assert await db.fetchval("SELECT count(*) FROM events WHERE workflow_id = $1", wf) == 0


async def test_live_lease_blocks_second_claim(pool, db):
    await new_workflow(db)
    async with pool.acquire() as conn:
        assert await store.claim_workflow_task(conn, "w1", 5.0) is not None
        assert await store.claim_workflow_task(conn, "w2", 5.0) is None


async def test_expired_lease_is_reclaimed_with_new_epoch(pool, db):
    await new_workflow(db)
    async with pool.acquire() as conn:
        first = await store.claim_workflow_task(conn, "w1", 0.05)
        await asyncio.sleep(0.1)
        second = await store.claim_workflow_task(conn, "w2", 5.0)
    assert second.task_id == first.task_id
    assert second.epoch == first.epoch + 1
    assert second.attempt == first.attempt + 1


async def test_concurrent_claimers_get_different_tasks(pool, db):
    for _ in range(5):
        await new_workflow(db)

    async def claim(i):
        async with pool.acquire() as conn:
            return await store.claim_workflow_task(conn, f"w{i}", 5.0)

    claims = await asyncio.gather(*(claim(i) for i in range(5)))
    assert len({c.task_id for c in claims}) == 5


async def test_wake_ups_coalesce_into_one_pending_task(db):
    wf = await new_workflow(db)
    await store.wake_workflow(db, wf)
    await store.wake_workflow(db, wf)
    assert await db.fetchval("SELECT count(*) FROM tasks WHERE workflow_id = $1", wf) == 1


async def test_stale_activity_result_is_dropped(pool, db):
    wf = await db.fetchval("INSERT INTO workflows (type) VALUES ('t') RETURNING id")
    await store.enqueue_activity(db, wf, {"name": "x", "args": [], "scheduled_seq": 1, "idempotency_key": "k"})
    async with pool.acquire() as conn:
        a = await store.claim_activity_task(conn, "a1", 0.05)
        await asyncio.sleep(0.1)
        b = await store.claim_activity_task(conn, "a2", 5.0)
        assert b.attempt == a.attempt + 1
        assert await store.record_activity_result(conn, a, "a1", "ActivityCompleted", {"scheduled_seq": 1}) is False
        assert await store.record_activity_result(conn, b, "a2", "ActivityCompleted", {"scheduled_seq": 1}) is True
    assert await db.fetchval("SELECT count(*) FROM events WHERE workflow_id = $1", wf) == 1


async def test_second_terminal_event_is_rejected(db):
    wf = await db.fetchval("INSERT INTO workflows (type) VALUES ('t') RETURNING id")
    await db.execute("INSERT INTO events (workflow_id, seq, type) VALUES ($1, 1, 'WorkflowCompleted')", wf)
    with pytest.raises(asyncpg.UniqueViolationError):
        await db.execute("INSERT INTO events (workflow_id, seq, type) VALUES ($1, 2, 'WorkflowCompleted')", wf)


async def test_delivery_takes_locks_in_the_same_order_as_claim(pool, db):
    """Regression: delivering an event used to lock the workflow row before the task,
    the opposite of claim, so the two could deadlock."""
    wf = await new_workflow(db)
    async with pool.acquire() as claimer, pool.acquire() as deliverer:
        claim_tx = claimer.transaction()
        await claim_tx.start()
        # The claim's first step: take the pending workflow task.
        await claimer.execute("UPDATE tasks SET claimed_by = 'w1' WHERE workflow_id = $1", wf)

        async def deliver():
            async with deliverer.transaction():
                return await store.deliver(deliverer, wf, "SignalReceived", {"name": "s", "payload": 1})

        delivering = asyncio.create_task(deliver())
        await asyncio.sleep(0.3)  # let delivery block on the task the claim holds
        # The claim's second step: lock the workflow row. Deadlocks if delivery holds it.
        await claimer.execute("UPDATE workflows SET epoch = epoch + 1 WHERE id = $1", wf)
        await claim_tx.commit()

        assert await asyncio.wait_for(delivering, 5) == 1
