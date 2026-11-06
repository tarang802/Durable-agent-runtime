"""Replay time vs history length.

Loads a workflow's history from Postgres and replays the workflow function against it,
as a workflow worker does on every step. Continue-As-New caps history length, so the row
at the configured limit (default 1,000 events) is the worst case for a workflow using it.
"""

import asyncio
import uuid

from durably.context import Context, replay
from durably.db import connect
from durably.store import load_history
from experiments.app import steps
from experiments.common import fresh_schema, table, timed

SIZES = [10, 100, 1_000, 5_000, 10_000]


async def main() -> str:
    await fresh_schema()
    conn = await connect()
    rows = []
    for size in SIZES:
        n = (size - 1) // 2  # WorkflowStarted + one Scheduled/Completed pair per activity
        wf = await conn.fetchval("INSERT INTO workflows (type, input) VALUES ('steps', $1) RETURNING id", n + 1)
        events = [(wf, 1, "WorkflowStarted", {"input": n + 1})]
        for i in range(n):
            seq = 2 + 2 * i
            events.append((wf, seq, "ActivityScheduled", {"name": "noop", "args": [i], "retry": None, "timeout": None}))
            events.append((wf, seq + 1, "ActivityCompleted", {"scheduled_seq": seq, "result": None}))
        await conn.executemany("INSERT INTO events (workflow_id, seq, type, payload) VALUES ($1, $2, $3, $4)", events)

        history = await load_history(conn, wf)
        load_ms = await timed(lambda: load_history(conn, wf))
        replay_ms = await timed(lambda: replay(steps, Context(uuid.uuid4(), history, max_history=10**9), n + 1))
        rows.append([f"{len(history):,}", f"{load_ms:.1f}", f"{replay_ms:.1f}", f"{load_ms + replay_ms:.1f}"])
    await conn.close()
    return table("Replay time vs history length (median of 5)",
                 ["Events in history", "Load (ms)", "Replay (ms)", "Total (ms)"], rows)


if __name__ == "__main__":
    print(asyncio.run(main()))
