"""End-to-end behaviour of start -> workflow worker -> activity worker -> result."""

import pytest

import durably
from durably.testing import FakeLLM

llm = FakeLLM()
calls: list[str] = []
seen_keys: list[str] = []


@pytest.fixture(autouse=True)
def reset_fakes():
    llm.reset()
    calls.clear()
    seen_keys.clear()


@durably.activity("llm")
async def llm_activity(prompt, *, idempotency_key):
    return await llm.complete(prompt, idempotency_key=idempotency_key)


@durably.activity("record")
async def record_activity(label, *, idempotency_key):
    calls.append(label)
    seen_keys.append(idempotency_key)
    return f"{label}-done"


@durably.workflow("two_steps")
async def two_steps(ctx, input):
    first = await ctx.activity("record", ["a"])
    second = await ctx.activity("record", [first])
    return {"first": first, "second": second, "input": input}


@durably.workflow("flaky_llm")
async def flaky_llm(ctx, input):
    return await ctx.activity("llm", [input], retry={"initial_interval": 0.01})


@durably.workflow("catches_failure")
async def catches_failure(ctx, input):
    try:
        await ctx.activity("llm", [input], retry={"max_attempts": 2, "initial_interval": 0.01})
    except durably.ActivityError as e:
        return {"caught": e.message}


async def test_runs_two_activities(client, workers):
    workers()
    wf = await client.start(two_steps, {"x": 1})

    result = await client.result(wf, timeout=10)

    assert result == {"first": "a-done", "second": "a-done-done", "input": {"x": 1}}
    assert calls == ["a", "a-done"]
    assert [e.type for e in await client.history(wf)] == [
        "WorkflowStarted",
        "ActivityScheduled", "ActivityCompleted",
        "ActivityScheduled", "ActivityCompleted",
        "WorkflowCompleted",
    ]


async def test_duplicate_start_returns_existing_workflow(client, workers):
    first = await client.start(two_steps, {"x": 1}, idempotency_key="job-1")
    second = await client.start(two_steps, {"x": 2}, idempotency_key="job-1")
    assert first == second

    workers()
    assert (await client.result(first, timeout=10))["input"] == {"x": 1}
    assert calls == ["a", "a-done"]  # ran once


async def test_history_seqs_are_contiguous(client, workers):
    workers()
    wf = await client.start(two_steps)
    await client.result(wf, timeout=10)
    assert [e.seq for e in await client.history(wf)] == [1, 2, 3, 4, 5, 6]


async def test_activity_receives_stable_idempotency_key(client, workers):
    workers()
    wf = await client.start(two_steps)
    await client.result(wf, timeout=10)

    scheduled = [e.seq for e in await client.history(wf) if e.type == "ActivityScheduled"]
    assert seen_keys == [f"{wf}:{seq}" for seq in scheduled]


async def test_failed_activity_is_retried_with_same_key(client, workers):
    llm.fail_next(2)
    workers()
    wf = await client.start(flaky_llm, "hello")

    assert await client.result(wf, timeout=10) == "llm(hello)"
    assert llm.calls == 3
    [key] = llm.calls_by_key
    assert llm.calls_by_key[key] == 3
    assert llm.effects_by_key[key] == 1


async def test_exhausted_retries_raise_activity_error_in_workflow(client, workers):
    llm.fail_next(10)
    workers()
    wf = await client.start(catches_failure, "hello")

    assert await client.result(wf, timeout=10) == {"caught": "FakeLLMError: injected failure"}
    assert llm.calls == 2


async def test_uncaught_activity_failure_fails_workflow(client, workers):
    llm.fail_next(10)
    workers()
    wf = await client.start(flaky_llm, "hello")

    with pytest.raises(durably.WorkflowFailedError, match="injected failure"):
        await client.result(wf, timeout=10)
    assert [e.type for e in await client.history(wf)][-1] == "WorkflowFailed"


@durably.workflow("stamped")
async def stamped(ctx, input):
    before = ctx.now()
    token = ctx.uuid4()
    await ctx.activity("record", ["x"])  # forces at least one more replay
    return {"before": before.isoformat(), "token": str(token), "again": str(ctx.uuid4()) != str(token)}


async def test_now_and_uuid_are_stable_across_replays(client, workers):
    workers()
    wf = await client.start(stamped)
    result = await client.result(wf, timeout=10)

    markers = [e.payload for e in await client.history(wf) if e.type == "MarkerRecorded"]
    assert [m["kind"] for m in markers] == ["now", "uuid4", "uuid4"]
    assert result["before"] == markers[0]["value"]
    assert result["token"] == markers[1]["value"]
    assert result["again"] is True
