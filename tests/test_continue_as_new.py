"""Continue-As-New: long-running workflows roll over to a fresh run with bounded history."""

import pytest

import durably

MAX_HISTORY = 10


@durably.activity("add")
async def add(n, *, idempotency_key):
    return n


@durably.workflow("counter")
async def counter(ctx, state):
    total, n = state["total"], state["n"]
    while n < state["target"]:
        total += await ctx.activity("add", [n])
        n += 1
        if ctx.should_continue_as_new():
            ctx.continue_as_new({"total": total, "n": n, "target": state["target"]})
    return total


@durably.workflow("mailbox")
async def mailbox(ctx, state):
    got = state["got"] + [await ctx.wait_signal("m")]
    if len(got) == state["want"]:
        return got
    ctx.continue_as_new({"got": got, "want": state["want"]})


@pytest.fixture
def start_workers(workers):
    return lambda: workers(max_history=MAX_HISTORY)


async def test_long_workflow_rolls_over_and_finishes(client, start_workers):
    start_workers()
    wf = await client.start(counter, {"total": 0, "n": 0, "target": 20})

    assert await client.result(wf, timeout=20) == sum(range(20))
    runs = await client.runs(wf)
    assert len(runs) > 1
    for run in runs:
        # Bounded. The workflow checks once per iteration, so a run can pass the limit by
        # one iteration (2 events) before the marker and the closing event: at most 13.
        assert len(await client.history(run)) <= MAX_HISTORY + 3


async def test_each_run_links_to_the_next(client, start_workers):
    start_workers()
    wf = await client.start(counter, {"total": 0, "n": 0, "target": 20})
    await client.result(wf, timeout=20)

    runs = await client.runs(wf)
    for prev, nxt in zip(runs, runs[1:]):
        assert (await client.history(prev))[-1].type == "WorkflowContinuedAsNew"
        assert (await client.history(nxt))[0].payload["continued_from"] == prev
    assert (await client.describe(runs[-1]))["status"] == "completed"


async def test_untaken_signals_carry_over_to_the_next_run(client, start_workers):
    wf = await client.start(mailbox, {"got": [], "want": 4})
    for i in range(4):  # all queued on the first run, which takes only one
        await client.signal(wf, "m", i)

    start_workers()
    assert await client.result(wf, timeout=20) == [0, 1, 2, 3]
    assert len(await client.runs(wf)) == 4


async def test_signal_to_an_old_run_id_reaches_the_latest_run(client, start_workers):
    start_workers()
    wf = await client.start(mailbox, {"got": [], "want": 3})
    for i in range(3):
        await client.signal(wf, "m", i)  # always the original id
    assert await client.result(wf, timeout=20) == [0, 1, 2]
