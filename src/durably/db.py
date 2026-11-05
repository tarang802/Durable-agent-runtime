"""Database access: connection settings and schema setup."""

import os
from importlib.resources import files

import asyncpg

DEFAULT_URL = "postgres://durably:durably@localhost:5433/durably"


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_URL)


def schema_sql() -> str:
    return files("durably").joinpath("schema.sql").read_text(encoding="utf-8")


async def init_db(conn: asyncpg.Connection) -> None:
    """Create tables, constraints and triggers. Idempotent."""
    await conn.execute(schema_sql())
