"""The replay engine on hand-built histories, without a database."""

import uuid

from durably.context import Command, Completed, Context, NonDeterministic, Suspended, replay
from durably.store import Event

WF = uuid.uuid4()


async def two_calls(ctx, input):
    a = await ctx.activity("a", [1])
    b = await ctx.activity("b", [a])
    return [a, b]


async def test_empty_history_schedules_first_activity():
    outcome = await replay(two_calls, Context(WF, []), None)
    assert isinstance(outcome, Suspended)
    assert outcome.command.payload["name"] == "a"


async def test_recorded_results_are_returned_not_rescheduled():
    history = [
        Event(1, "WorkflowStarted", {}),
        Event(2, "ActivityScheduled", {"name": "a", "args": [1]}),
        Event(3, "ActivityCompleted", {"scheduled_seq": 2, "result": "A"}),
        Event(4, "ActivityScheduled", {"name": "b", "args": ["A"]}),
        Event(5, "ActivityCompleted", {"scheduled_seq": 4, "result": "B"}),
    ]
    assert await replay(two_calls, Context(WF, history), None) == Completed(["A", "B"])


async def test_pending_activity_suspends_without_new_command():
    history = [Event(2, "ActivityScheduled", {"name": "a", "args": [1]})]
    assert await replay(two_calls, Context(WF, history), None) == Suspended(None)


async def test_changed_code_is_nondeterminism():
    history = [Event(2, "ActivityScheduled", {"name": "other", "args": [1]})]
    outcome = await replay(two_calls, Context(WF, history), None)
    assert isinstance(outcome, NonDeterministic)
    assert "other" in outcome.error


async def test_nondeterminism_cannot_be_swallowed_by_workflow_code():
    async def swallows(ctx, input):
        try:
            await ctx.activity("a", [1])
        except Exception:
            return "hid it"

    history = [Event(2, "ActivityScheduled", {"name": "a", "args": [2]})]
    assert isinstance(await replay(swallows, Context(WF, history), None), NonDeterministic)


async def naps(ctx, input):
    await ctx.sleep(30)
    return "awake"


async def test_sleep_records_a_timer_and_suspends():
    outcome = await replay(naps, Context(WF, []), None)
    assert outcome == Suspended(Command("TimerStarted", {"seconds": 30.0}))


async def test_sleep_waits_until_the_timer_fires():
    started = [Event(2, "TimerStarted", {"seconds": 30.0})]
    assert await replay(naps, Context(WF, started), None) == Suspended(None)
    fired = started + [Event(3, "TimerFired", {"started_seq": 2})]
    assert await replay(naps, Context(WF, fired), None) == Completed("awake")


async def test_changed_sleep_duration_is_nondeterminism():
    history = [Event(2, "TimerStarted", {"seconds": 10.0})]
    assert isinstance(await replay(naps, Context(WF, history), None), NonDeterministic)
