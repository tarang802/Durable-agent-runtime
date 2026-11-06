"""Recovery time vs lease length.

A workflow worker claims a workflow and dies at once (simulated by claiming its task and
abandoning it). A healthy worker is already running. Recovery time is from the claim to
the workflow's completion, on the database clock. It should track the lease length.
"""

import asyncio
import statistics

from durably import Client, store
from durably.db import create_pool
from experiments.common import fresh_schema, spawn_worker, stop, table

LEASES = [0.5, 1, 2, 5]
REPEATS = 3


async def run(lease: float) -> float:
    await fresh_schema()
    pool = await create_pool()
    client = Client(pool)
    workers = [await spawn_worker("workflow", lease=lease), await spawn_worker("activity", lease=lease)]
    try:
        await asyncio.sleep(2)  # healthy workers are up and polling
        async with pool.acquire() as conn:
            async with conn.transaction():
                wf = await store.create_workflow(conn, "steps", 1)
                # The "crashed" worker claims first, in the same transaction, then vanishes.
                claim = await store.claim_workflow_task(conn, "crashed-worker", lease)
                claimed_at = await conn.fetchval("SELECT now()")
            assert claim.workflow_id == wf
        await client.result(str(wf), timeout=60)
        async with pool.acquire() as conn:
            done_at = await conn.fetchval(
                "SELECT created_at FROM events WHERE workflow_id = $1 AND type = 'WorkflowCompleted'", wf)
        return (done_at - claimed_at).total_seconds()
    finally:
        await pool.close()
        await stop(*workers)


async def main() -> str:
    rows = []
    for lease in LEASES:
        samples = [await run(lease) for _ in range(REPEATS)]
        rows.append([f"{lease:g}", f"{statistics.median(samples):.2f}"])
    return table(f"Recovery time after a workflow worker dies (median of {REPEATS})",
                 ["Lease (s)", "Claim to completion (s)"], rows)


if __name__ == "__main__":
    print(asyncio.run(main()))
