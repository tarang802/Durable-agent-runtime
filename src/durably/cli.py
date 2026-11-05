"""`durably` command-line entry point."""

import argparse
import asyncio
import logging

from durably import worker
from durably.db import connect, init_db


async def _init_db(url: str | None) -> None:
    conn = await connect(url)
    try:
        await init_db(conn)
    finally:
        await conn.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="durably")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init-db", help="create durably's tables in Postgres")
    init.add_argument("--database-url", default=None, help="defaults to $DATABASE_URL")

    worker.add_arguments(sub.add_parser("worker", help="run a workflow or activity worker"))

    args = parser.parse_args(argv)
    if args.command == "init-db":
        asyncio.run(_init_db(args.database_url))
        print("durably: schema ready")
    elif args.command == "worker":
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        try:
            asyncio.run(worker.serve(args))
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
