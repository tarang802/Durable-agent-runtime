"""The replay engine on hand-built histories, without a database."""

import uuid
from datetime import datetime

from durably.context import Command, Completed, Context, ContinuedAsNew, NonDeterministic, Suspended, replay
from durably.store import Event

WF = uuid.uuid4()


async def two_calls(ctx, input):
    a = await ctx.activity("a", [1])
    b = await ctx.activity("b", [a])
    return [a, b]


async def test_empty_history_schedules_first_activity():
    outcome = await replay(two_calls, Context(WF, []), None)
    assert isinstance(outcome, Suspended)
    assert outcome.commands[0].payload["name"] == "a"


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
    assert await replay(two_calls, Context(WF, history), None) == Suspended([])


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
    assert outcome == Suspended([Command("TimerStarted", {"seconds": 30.0, "for_signal": None})])


async def test_sleep_waits_until_the_timer_fires():
    started = [Event(2, "TimerStarted", {"seconds": 30.0})]
    assert await replay(naps, Context(WF, started), None) == Suspended([])
    fired = started + [Event(3, "TimerFired", {"started_seq": 2})]
    assert await replay(naps, Context(WF, fired), None) == Completed("awake")


async def test_changed_sleep_duration_is_nondeterminism():
    history = [Event(2, "TimerStarted", {"seconds": 10.0})]
    assert isinstance(await replay(naps, Context(WF, history), None), NonDeterministic)


def signal(seq, name, payload):
    return Event(seq, "SignalReceived", {"name": name, "payload": payload})


async def waits_twice(ctx, input):
    return [await ctx.wait_signal("msg"), await ctx.wait_signal("msg")]


async def test_signals_are_taken_in_order_once_each():
    history = [signal(2, "msg", "a"), signal(3, "other", "x"), signal(4, "msg", "b"), signal(5, "msg", "c")]
    assert await replay(waits_twice, Context(WF, history), None) == Completed(["a", "b"])


async def test_wait_without_signal_suspends_without_a_command():
    assert await replay(waits_twice, Context(WF, [signal(2, "msg", "a")]), None) == Suspended([])


async def timed_then_untimed(ctx, input):
    first = await ctx.wait_signal("msg", timeout=5)
    second = await ctx.wait_signal("msg")
    return [first, second]


async def test_timed_wait_starts_a_timer_when_no_signal_yet():
    outcome = await replay(timed_then_untimed, Context(WF, []), None)
    assert outcome == Suspended([Command("TimerStarted", {"seconds": 5.0, "for_signal": "msg"})])


async def test_signal_before_the_timer_fires_wins():
    history = [Event(2, "TimerStarted", {"seconds": 5.0, "for_signal": "msg"}), signal(3, "msg", "a"),
               signal(4, "msg", "b")]
    assert await replay(timed_then_untimed, Context(WF, history), None) == Completed(["a", "b"])


async def test_timeout_returns_none_and_late_signal_goes_to_next_wait():
    history = [Event(2, "TimerStarted", {"seconds": 5.0, "for_signal": "msg"}),
               Event(3, "TimerFired", {"started_seq": 2}), signal(4, "msg", "late")]
    assert await replay(timed_then_untimed, Context(WF, history), None) == Completed([None, "late"])


async def test_signal_already_waiting_needs_no_timer():
    # Run 1 saw the signal immediately and started no timer; replays must agree.
    history = [signal(2, "msg", "a"), signal(3, "msg", "b")]
    assert await replay(timed_then_untimed, Context(WF, history), None) == Completed(["a", "b"])


async def uses_markers(ctx, input):
    stamp = ctx.now()
    roll = ctx.random()
    ident = ctx.uuid4()
    await ctx.activity("a", [roll])
    return [stamp.isoformat(), roll, str(ident)]


async def test_markers_are_recorded_without_suspending():
    outcome = await replay(uses_markers, Context(WF, []), None)
    assert [c.type for c in outcome.commands] == ["MarkerRecorded"] * 3 + ["ActivityScheduled"]
    assert [c.payload["kind"] for c in outcome.commands[:3]] == ["now", "random", "uuid4"]
    assert outcome.commands[3].payload["args"] == [outcome.commands[1].payload["value"]]


async def test_markers_replay_their_recorded_values():
    first = await replay(uses_markers, Context(WF, []), None)
    history = [Event(i + 2, c.type, c.payload) for i, c in enumerate(first.commands)]
    history.append(Event(6, "ActivityCompleted", {"scheduled_seq": 5, "result": None}))

    [stamp, roll, ident] = [c.payload["value"] for c in first.commands[:3]]
    outcome = await replay(uses_markers, Context(WF, history), None)
    assert outcome == Completed([datetime.fromisoformat(stamp).isoformat(), roll, ident])


async def test_changed_marker_kind_is_nondeterminism():
    history = [Event(2, "MarkerRecorded", {"kind": "random", "value": 0.5})]
    assert isinstance(await replay(uses_markers, Context(WF, history), None), NonDeterministic)


async def checks_for_rollover(ctx, input):
    for i in range(3):
        await ctx.activity("a", [i])
        if ctx.should_continue_as_new():
            ctx.continue_as_new({"from": i})
    return "done"


def two_activities_done():
    return [Event(2, "ActivityScheduled", {"name": "a", "args": [0]}),
            Event(3, "ActivityCompleted", {"scheduled_seq": 2, "result": None})]


async def test_should_continue_as_new_is_false_below_the_limit():
    outcome = await replay(checks_for_rollover, Context(WF, two_activities_done(), max_history=100), None)
    assert [c.type for c in outcome.commands] == ["ActivityScheduled"]  # kept going to activity 1


async def test_reaching_the_limit_records_a_marker_and_continues_as_new():
    outcome = await replay(checks_for_rollover, Context(WF, two_activities_done(), max_history=2), None)
    assert isinstance(outcome, ContinuedAsNew)
    assert outcome.input == {"from": 0}
    assert [c.payload["kind"] for c in outcome.commands] == ["continue_as_new_suggested"]


async def test_replay_answers_as_the_original_run_did():
    # Originally under the limit at i=0 (no marker), and history is longer now: still False there.
    history = two_activities_done() + [Event(4, "ActivityScheduled", {"name": "a", "args": [1]})]
    assert await replay(checks_for_rollover, Context(WF, history, max_history=2), None) == Suspended([])


async def test_untaken_signals_are_carried_to_the_next_run():
    async def take_one_then_roll(ctx, input):
        await ctx.wait_signal("m")
        ctx.continue_as_new(None)

    history = [signal(2, "m", 1), signal(3, "other", "x"), signal(4, "m", 2)]
    outcome = await replay(take_one_then_roll, Context(WF, history), None)
    assert outcome.signals == [{"name": "other", "payload": "x"}, {"name": "m", "payload": 2}]
