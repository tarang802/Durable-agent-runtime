import asyncpg
import pytest

from durably.db import init_db


async def new_workflow(db, key=None):
    return await db.fetchval(
        "INSERT INTO workflows (type, idempotency_key) VALUES ('t', $1) RETURNING id", key
    )


async def test_tables_exist(db):
    names = {
        r["tablename"]
        for r in await db.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    }
    assert {"workflows", "events", "tasks"} <= names


async def test_init_db_is_idempotent(db):
    await init_db(db)


async def test_event_seq_is_unique_per_workflow(db):
    wf = await new_workflow(db)
    await db.execute("INSERT INTO events (workflow_id, seq, type) VALUES ($1, 1, 'WorkflowStarted')", wf)
    with pytest.raises(asyncpg.UniqueViolationError):
        await db.execute("INSERT INTO events (workflow_id, seq, type) VALUES ($1, 1, 'MarkerRecorded')", wf)


@pytest.mark.parametrize("stmt", [
    "UPDATE events SET payload = '{\"x\": 1}'",
    "DELETE FROM events",
])
async def test_events_are_append_only(db, stmt):
    wf = await new_workflow(db)
    await db.execute("INSERT INTO events (workflow_id, seq, type) VALUES ($1, 1, 'WorkflowStarted')", wf)
    with pytest.raises(asyncpg.RaiseError, match="append-only"):
        await db.execute(stmt)


async def test_idempotency_key_is_unique(db):
    await new_workflow(db, "k1")
    with pytest.raises(asyncpg.UniqueViolationError):
        await new_workflow(db, "k1")


async def test_unknown_event_type_rejected(db):
    wf = await new_workflow(db)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute("INSERT INTO events (workflow_id, seq, type) VALUES ($1, 1, 'Bogus')", wf)


async def test_database_calls_time_out_instead_of_hanging(db):
    from durably.db import create_pool

    pool = await create_pool(command_timeout=0.3)
    try:
        with pytest.raises(TimeoutError):
            await pool.fetchval("SELECT pg_sleep(5)")
        assert await pool.fetchval("SELECT 1") == 1  # the pool recovers
    finally:
        await pool.close()
