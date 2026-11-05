import asyncio

import pytest

from durably import ActivityWorker, Client, WorkflowWorker
from durably.db import connect, create_pool, init_db


@pytest.fixture
async def db():
    """A connection to a freshly reset schema, so every test starts empty."""
    conn = await connect()
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await init_db(conn)
    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
async def pool(db):
    p = await create_pool(min_size=1, max_size=10)
    try:
        yield p
    finally:
        await p.close()


@pytest.fixture
async def client(pool):
    return Client(pool)


@pytest.fixture
async def workers(pool):
    """Call to start one workflow worker and one activity worker in this process."""
    stop = asyncio.Event()
    tasks = []

    def start(lease: float = 5.0):
        for cls in (WorkflowWorker, ActivityWorker):
            tasks.append(asyncio.create_task(cls(pool, lease=lease, poll=0.01).run(stop)))

    yield start
    stop.set()
    await asyncio.gather(*tasks)
