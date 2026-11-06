"""Paid LLM calls on resume vs a naive full restart.

An agent makes 5 paid LLM calls in a row. Its activity worker is killed right after
recording step k (FAILPOINT=after_record_before_ack@k), and a new worker finishes the run.
The naive baseline is the same loop as a plain script that starts over after the crash.
"""

import asyncio

from durably import Client
from durably.db import connect, create_pool
from durably.failpoints import EXIT_CODE
from durably.testing import SharedFakeLLM
from experiments.common import fresh_schema, spawn_worker, stop, table

STEPS = 5


async def durably_run(crash_after: int) -> tuple[int, int]:
    await fresh_schema()
    conn = await connect()
    await SharedFakeLLM.install(conn)
    await conn.close()
    pool = await create_pool()
    client = Client(pool)
    procs = []
    try:
        wf = await client.start("agent", STEPS)
        procs.append(await spawn_worker("workflow", lease=1))
        procs.append(crashing := await spawn_worker("activity", lease=1,
                                                    failpoint=f"after_record_before_ack@{crash_after}"))
        assert await asyncio.wait_for(crashing.wait(), 30) == EXIT_CODE
        procs.append(await spawn_worker("activity", lease=1))
        await client.result(wf, timeout=60)
        stats = await SharedFakeLLM().stats()
        return sum(stats.calls_by_key.values()), sum(stats.effects_by_prompt.values())
    finally:
        await pool.close()
        await stop(*procs)


def naive_calls(crash_after: int) -> int:
    calls = 0
    for attempt in range(2):  # the crashed run, then a full restart
        for step in range(STEPS):
            calls += 1  # each call is a new, paid request: there is nothing to resume from
            if attempt == 0 and step + 1 == crash_after:
                break
    return calls


async def main() -> str:
    rows = []
    for k in range(1, STEPS):
        calls, paid = await durably_run(k)
        rows.append([k, naive_calls(k), calls, paid])
    return table(f"Paid LLM calls for a {STEPS}-step agent killed after step k",
                 ["Killed after step", "Naive restart: paid calls", "durably: calls", "durably: paid calls"], rows)


if __name__ == "__main__":
    print(asyncio.run(main()))
