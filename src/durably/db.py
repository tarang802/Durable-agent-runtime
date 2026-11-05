"""Database access: connection settings and schema setup."""

import json
import os
from importlib.resources import files

import asyncpg

DEFAULT_URL = "postgres://durably:durably@localhost:5433/durably"


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_URL)


def schema_sql() -> str:
    return files("durably").joinpath("schema.sql").read_text(encoding="utf-8")


async def _init_connection(conn: asyncpg.Connection) -> None:
    # jsonb columns round-trip as Python dicts/lists.
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def connect(url: str | None = None) -> asyncpg.Connection:
    conn = await asyncpg.connect(url or database_url())
    await _init_connection(conn)
    return conn


async def create_pool(url: str | None = None, **kwargs) -> asyncpg.Pool:
    return await asyncpg.create_pool(url or database_url(), init=_init_connection, **kwargs)


async def init_db(conn: asyncpg.Connection) -> None:
    """Create tables, constraints and triggers. Idempotent."""
    await conn.execute(schema_sql())
