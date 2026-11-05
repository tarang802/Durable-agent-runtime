"""The replay engine on hand-built histories, without a database."""

import uuid

from durably.context import Completed, Context, NonDeterministic, Suspended, replay
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
