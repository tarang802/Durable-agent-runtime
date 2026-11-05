"""`durably` command-line entry point."""

import argparse
import asyncio

import asyncpg

from durably.db import database_url, init_db


async def _init_db(url: str) -> None:
    conn = await asyncpg.connect(url)
    try:
        await init_db(conn)
    finally:
        await conn.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="durably")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init-db", help="create durably's tables in Postgres")
    init.add_argument("--database-url", default=None, help="defaults to $DATABASE_URL")

    args = parser.parse_args(argv)
    if args.command == "init-db":
        url = args.database_url or database_url()
        asyncio.run(_init_db(url))
        print("durably: schema ready")


if __name__ == "__main__":
    main()
