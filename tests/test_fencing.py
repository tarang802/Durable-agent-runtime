"""A paused worker wakes up after another worker took over.

Worker A replays the workflow and freezes (SIGSTOP) just before writing. Its lease
expires, worker B takes over and schedules the same step. Then A resumes and tries to
write what it computed before the pause. A lease check before the write cannot catch
this, because A already passed any such check; only the epoch condition inside the write
can.
"""

import asyncio

TOPIC = "fencing"
PLAN = f"plan {TOPIC}"


async def scheduled(client, wf, prompt) -> int:
    return sum(
        1 for e in await client.history(wf)
        if e.type == "ActivityScheduled" and e.payload["args"] == [prompt]
    )


async def _at_least(client, wf, prompt, n) -> bool:
    return await scheduled(client, wf, prompt) >= n


async def wait_for(predicate, timeout: float = 15) -> None:
    async def poll():
        while not await predicate():
            await asyncio.sleep(0.05)
    await asyncio.wait_for(poll(), timeout)


async def run_stale_worker_scenario(client, spawn, *, fencing: bool) -> str:
    wf = await client.start("research", TOPIC)
    stale = await spawn("workflow", failpoint="before_append:pause", fencing=fencing, lease=1.0)
    await stale.wait_stopped()

    # A is frozen holding a decision ("schedule the plan step"); B takes over after A's lease.
    await spawn("workflow", lease=5.0)
    await spawn("activity")
    await wait_for(lambda: _at_least(client, wf, PLAN, 1))

    stale.resume()
    return wf


async def test_stale_worker_write_rejected(client, spawn, shared_llm):
    wf = await run_stale_worker_scenario(client, spawn, fencing=True)

    assert await client.result(wf, timeout=20) == f"llm(summarize llm({PLAN}))"
    assert await scheduled(client, wf, PLAN) == 1  # A's late write was rejected
    stats = await shared_llm.stats()
    assert stats.effects_by_prompt[PLAN] == 1


async def test_without_fencing_stale_worker_corrupts_history(client, spawn, shared_llm):
    """The same scenario with the epoch check removed: A's stale write lands."""
    wf = await run_stale_worker_scenario(client, spawn, fencing=False)

    await wait_for(lambda: _at_least(client, wf, PLAN, 2))
    assert await scheduled(client, wf, PLAN) == 2  # the same step, scheduled twice

    async def billed_twice() -> bool:
        return (await shared_llm.stats()).effects_by_prompt[PLAN] == 2
    await wait_for(billed_twice)  # and the paid call really ran twice, under two keys
