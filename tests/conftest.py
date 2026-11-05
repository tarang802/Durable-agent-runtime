import asyncpg
import pytest

from durably.db import database_url, init_db


@pytest.fixture
async def db():
    """A connection to a freshly reset schema, so every test starts empty."""
    conn = await asyncpg.connect(database_url())
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await init_db(conn)
    try:
        yield conn
    finally:
        await conn.close()
