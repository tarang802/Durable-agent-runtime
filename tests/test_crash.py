"""Crash recovery with real worker processes.

Each test kills (or freezes) a worker at a failpoint, starts a replacement, and checks
that the workflow finishes without repeating completed work.
"""

import pytest

from durably.failpoints import EXIT_CODE

# The "research" workflow in tests/crash_app.py, which only the worker processes import.
TOPIC = "durable execution"
STEPS = {f"plan {TOPIC}", f"summarize llm(plan {TOPIC})"}


def expected_result(topic: str) -> str:
    return f"llm(summarize llm(plan {topic}))"


async def test_resume_does_not_repeat_completed_activity(client, spawn, shared_llm):
    wf = await client.start("research", TOPIC)
    await spawn("workflow")
    crashing = await spawn("activity", failpoint="after_record_before_ack")

    # The activity worker dies right after recording the first activity's result.
    assert await crashing.exit_code() == EXIT_CODE
    await spawn("activity")

    assert await client.result(wf, timeout=20) == expected_result(TOPIC)
    stats = await shared_llm.stats()
    assert stats.calls_by_prompt[f"plan {TOPIC}"] == 1  # not re-run after the crash
    assert stats.effects_by_prompt == {step: 1 for step in STEPS}


async def test_workflow_worker_crash_is_recovered_after_lease_expiry(client, spawn, shared_llm):
    wf = await client.start("research", TOPIC)
    crashing = await spawn("workflow", failpoint="after_claim")
    await spawn("activity")

    assert await crashing.exit_code() == EXIT_CODE
    await spawn("workflow")

    assert await client.result(wf, timeout=20) == expected_result(TOPIC)
    assert (await client.describe(wf))["epoch"] >= 2  # the replacement took a new epoch


# (worker kind, failpoint, duplicate provider calls expected)
CRASH_MATRIX = [
    ("activity", "before_claim", 0),
    ("activity", "after_claim", 0),
    ("activity", "before_activity", 0),
    # The effect happened but was not recorded: the retry calls the provider again with
    # the same idempotency key. At-least-once, but billed once.
    ("activity", "after_effect_before_record", 1),
    ("activity", "after_record_before_ack", 0),
    ("workflow", "before_claim", 0),
    ("workflow", "after_claim", 0),
    ("workflow", "before_append", 0),
    ("workflow", "after_record_before_ack", 0),
]


@pytest.mark.parametrize(("kind", "point", "duplicates"), CRASH_MATRIX, ids=[f"{k}-{p}" for k, p, _ in CRASH_MATRIX])
async def test_crash_matrix(client, spawn, shared_llm, kind, point, duplicates):
    other = "activity" if kind == "workflow" else "workflow"
    wf = await client.start("research", TOPIC)
    await spawn(other)
    crashing = await spawn(kind, failpoint=point)

    assert await crashing.exit_code() == EXIT_CODE
    await spawn(kind)

    assert await client.result(wf, timeout=20) == expected_result(TOPIC)
    stats = await shared_llm.stats()
    print(f"\ncrash {kind}:{point}: duplicate provider calls = {stats.duplicate_calls}")
    assert stats.duplicate_calls == duplicates
    # G6: every step is billed exactly once, whatever the crash point.
    assert stats.effects_by_prompt == {step: 1 for step in STEPS}


async def test_timer_fires_once_after_crash_while_firing(client, spawn, shared_llm):
    wf = await client.start("nap", TOPIC)
    crashing = await spawn("workflow", failpoint="during_timer_fire")
    await spawn("activity")

    # The worker dies inside the transaction that fires the timer, so nothing is written.
    assert await crashing.exit_code() == EXIT_CODE
    await spawn("workflow")

    assert await client.result(wf, timeout=20) == f"llm(after nap {TOPIC})"
    assert [e.type for e in await client.history(wf)].count("TimerFired") == 1  # G7
