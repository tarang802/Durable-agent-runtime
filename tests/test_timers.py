"""Durable timers: ctx.sleep."""

import time

import durably


@durably.workflow("short_nap")
async def short_nap(ctx, seconds):
    await ctx.sleep(seconds)
    return "awake"


async def test_sleep_resumes_after_the_timer(client, workers):
    workers()
    started = time.monotonic()
    wf = await client.start(short_nap, 0.3)

    assert await client.result(wf, timeout=10) == "awake"
    assert time.monotonic() - started >= 0.3
    assert [e.type for e in await client.history(wf)] == [
        "WorkflowStarted", "TimerStarted", "TimerFired", "WorkflowCompleted",
    ]


async def test_fired_timer_leaves_no_task_behind(client, workers, pool):
    workers()
    wf = await client.start(short_nap, 0.2)
    assert await client.result(wf, timeout=10) == "awake"

    # The timer is a row with a due time, not a sleeping coroutine: nothing is left behind.
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM tasks WHERE kind = 'timer'") == 0
