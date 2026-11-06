"""Run the research agent end to end in one process.

    docker compose run --rm tests python -m examples.research_agent.run

Workers run in this process for convenience. In production run them as separate
processes (see the README next to this file).
"""

import asyncio
import uuid

import durably
from durably.db import connect, create_pool, init_db
from examples.research_agent.app import research_agent


async def main() -> None:
    conn = await connect()
    await init_db(conn)  # idempotent: creates durably's tables if missing
    await conn.close()

    pool = await create_pool()
    client = durably.Client(pool)
    stop = asyncio.Event()
    workers = [durably.WorkflowWorker(pool, poll=0.05), durably.ActivityWorker(pool, poll=0.05)]
    running = [asyncio.create_task(w.run(stop)) for w in workers]

    try:
        wf = await client.start(
            research_agent, {"topic": "durable execution", "email": "reader@example.com"},
            idempotency_key=f"research-demo-{uuid.uuid4()}",
        )
        print(f"started workflow {wf}")

        # Wait until the agent has finished its research and is waiting for approval.
        while sum(e.type == "ActivityCompleted" for e in await client.history(wf)) < 5:
            await asyncio.sleep(0.1)
        print("summary ready; approving")
        await client.signal(wf, "approval", {"approved": True, "by": "sam"})

        result = await client.result(wf, timeout=60)
        print(f"\nresult: {result}\n\nhistory:")
        for event in await client.history(wf):
            print(f"  {event.seq:>2}  {event.type}")
    finally:
        stop.set()
        await asyncio.gather(*running)
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
