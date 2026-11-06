"""Database access: connection settings and schema setup."""

import json
import os
from importlib.resources import files

import asyncpg

DEFAULT_URL = "postgres://durably:durably@localhost:5433/durably"

# No database call waits forever: a connection that silently stops answering (a network
# partition, a failed-over primary) raises TimeoutError, and the caller reconnects.
DEFAULT_COMMAND_TIMEOUT_S = 60.0
DEFAULT_CONNECT_TIMEOUT_S = 10.0


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_URL)


def schema_sql() -> str:
    return files("durably").joinpath("schema.sql").read_text(encoding="utf-8")


async def _init_connection(conn: asyncpg.Connection) -> None:
    # jsonb columns round-trip as Python dicts/lists.
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def connect(url: str | None = None, *, command_timeout: float = DEFAULT_COMMAND_TIMEOUT_S) -> asyncpg.Connection:
    conn = await asyncpg.connect(url or database_url(), command_timeout=command_timeout,
                                 timeout=DEFAULT_CONNECT_TIMEOUT_S)
    await _init_connection(conn)
    return conn


async def create_pool(url: str | None = None, *, command_timeout: float = DEFAULT_COMMAND_TIMEOUT_S,
                      **kwargs) -> asyncpg.Pool:
    """A connection pool whose statements time out after `command_timeout` seconds."""
    return await asyncpg.create_pool(url or database_url(), init=_init_connection, command_timeout=command_timeout,
                                     timeout=min(command_timeout, DEFAULT_CONNECT_TIMEOUT_S), **kwargs)


async def init_db(conn: asyncpg.Connection) -> None:
    """Create tables, constraints and triggers. Idempotent."""
    await conn.execute(schema_sql())
