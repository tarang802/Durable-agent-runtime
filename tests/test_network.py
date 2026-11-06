"""Workers survive losing their database connection mid-workflow (via Toxiproxy)."""

import asyncio

TOPIC = "network"
STEPS = {f"plan {TOPIC}", f"summarize llm(plan {TOPIC})"}


async def test_workers_survive_a_dropped_database_connection(client, spawn, shared_llm, pg_proxy):
    wf = await client.start("slow_research", TOPIC)
    await spawn("workflow", database_url=pg_proxy.url)
    await spawn("activity", database_url=pg_proxy.url)

    # Cut while the first activity is running (it takes 0.5 s), so recording its result
    # fails and the workers must reconnect and recover.
    while not any(e.type == "ActivityScheduled" for e in await client.history(wf)):
        await asyncio.sleep(0.02)
    await pg_proxy.cut(seconds=2)

    assert await client.result(wf, timeout=30) == f"llm(summarize llm(plan {TOPIC}))"
    stats = await shared_llm.stats()
    assert stats.effects_by_prompt == {step: 1 for step in STEPS}  # nothing billed twice
