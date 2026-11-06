"""Activity throughput vs number of worker processes.

Queues 2,000 no-op activity tasks (one per workflow), then lets N worker processes drain
them. Throughput is measured on the database clock: tasks / (last result recorded - tasks
became visible). Each task is a full claim, run and record.
"""

import asyncio

from durably.db import connect
from experiments.common import fresh_schema, spawn_worker, stop, table

TASKS = 2_000
WORKERS = [1, 2, 4, 8]


async def run(workers: int) -> float:
    await fresh_schema()
    procs = [await spawn_worker("activity") for _ in range(workers)]
    conn = await connect()
    try:
        await asyncio.sleep(2)  # let the processes start and begin polling
        async with conn.transaction():
            ids = [r["id"] for r in await conn.fetch(
                "INSERT INTO workflows (type) SELECT 'bench' FROM generate_series(1, $1) RETURNING id", TASKS)]
            visible_at = await conn.fetchval("SELECT now() + interval '1 second'")
            await conn.executemany(
                "INSERT INTO tasks (workflow_id, kind, visible_at, payload) VALUES ($1, 'activity_task', $2, $3)",
                [(wf, visible_at, {"name": "noop", "args": [], "scheduled_seq": 2, "idempotency_key": f"{wf}:2"})
                 for wf in ids],
            )
        while await conn.fetchval("SELECT count(*) FROM tasks WHERE kind = 'activity_task'"):
            await asyncio.sleep(0.2)
        seconds = await conn.fetchval(
            "SELECT extract(epoch FROM max(created_at) - $1) FROM events WHERE type = 'ActivityCompleted'", visible_at)
        return TASKS / float(seconds)
    finally:
        await conn.close()
        await stop(*procs)


async def main() -> str:
    rows = [[n, f"{await run(n):,.0f}"] for n in WORKERS]
    return table(f"Activity throughput vs worker processes ({TASKS:,} tasks)",
                 ["Worker processes", "Tasks per second"], rows)


if __name__ == "__main__":
    print(asyncio.run(main()))
