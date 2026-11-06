"""Signals: client.signal and ctx.wait_signal."""

import asyncio

import pytest

import durably


@durably.workflow("approval")
async def approval(ctx, input):
    decision = await ctx.wait_signal("decision")
    return {"approved": decision["approved"], "by": decision["by"]}


@durably.workflow("inbox")
async def inbox(ctx, count):
    return [await ctx.wait_signal("msg") for _ in range(count)]


@durably.workflow("patient")
async def patient(ctx, input):
    first = await ctx.wait_signal("ping", timeout=0.3)
    second = await ctx.wait_signal("ping")
    return [first, second]


async def test_signal_resumes_a_waiting_workflow(client, workers):
    workers()
    wf = await client.start(approval)
    await client.signal(wf, "decision", {"approved": True, "by": "sam"})
    assert await client.result(wf, timeout=10) == {"approved": True, "by": "sam"}


async def test_signals_sent_before_any_worker_runs_are_kept_in_order(client, workers):
    wf = await client.start(inbox, 3)
    for i in range(3):
        await client.signal(wf, "msg", i)

    workers()
    assert await client.result(wf, timeout=10) == [0, 1, 2]


async def test_wait_with_timeout_returns_none_then_later_signal_is_received(client, workers):
    workers()
    wf = await client.start(patient)

    async def timed_out() -> bool:
        return "TimerFired" in [e.type for e in await client.history(wf)]

    while not await timed_out():
        await asyncio.sleep(0.05)
    await client.signal(wf, "ping", "late")

    assert await client.result(wf, timeout=10) == [None, "late"]


async def test_signal_to_finished_workflow_raises(client, workers):
    workers()
    wf = await client.start(inbox, 0)
    await client.result(wf, timeout=10)
    with pytest.raises(durably.WorkflowNotRunningError):
        await client.signal(wf, "msg", "too late")


async def test_concurrent_signals_and_claims_do_not_deadlock(client, workers):
    """Many workflows signalled while workers are claiming them all complete."""
    workers()

    async def start_and_signal() -> str:
        # Signal immediately, while a worker is claiming the brand-new workflow.
        wf = await client.start(inbox, 3)
        await asyncio.gather(*(client.signal(wf, "msg", i) for i in range(3)))
        return wf

    wfs = await asyncio.gather(*(start_and_signal() for _ in range(50)))
    results = await asyncio.gather(*(client.result(wf, timeout=30) for wf in wfs))
    assert all(sorted(r) == [0, 1, 2] for r in results)
