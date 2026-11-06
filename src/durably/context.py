"""The replay engine.

A workflow task reruns the workflow function from the top against its history. Each
`ctx.*` call takes the next command position:

- history has a command there: it must match the call (else NonDeterminismError); if
  its result is recorded, return it without running anything (G3);
- history is exhausted: this is a new command; record it. Commands that need a result
  (activities, timers) then suspend; markers (`now`, `random`, `uuid4`) do not, so one
  workflow task can record several new commands.

Suspending raises `_Suspend`, which unwinds the function. Nothing stays parked in memory:
the next event wakes a fresh replay.
"""

import json
import random
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, NoReturn
from uuid import UUID

from durably.errors import ActivityError, NonDeterminismError
from durably.store import Event

COMMAND_TYPES = {"ActivityScheduled", "TimerStarted", "MarkerRecorded"}
# Result event type -> payload field naming the command it answers.
RESULT_TYPES = {"ActivityCompleted": "scheduled_seq", "ActivityFailed": "scheduled_seq", "TimerFired": "started_seq"}


class _ContinueAsNew(BaseException):
    def __init__(self, input: Any):
        self.input = input


class _Suspend(BaseException):
    """Unwinds the workflow when it needs a result that is not in history yet.

    A BaseException so that `except Exception` in workflow code does not swallow it.
    """


@dataclass
class Command:
    type: str
    payload: dict


def _normalize(value: Any) -> Any:
    """What a value looks like after a round trip through jsonb, for comparing with history."""
    return json.loads(json.dumps(value))


DEFAULT_MAX_HISTORY = 1000


class Context:
    def __init__(self, workflow_id: UUID, history: list[Event], max_history: int = DEFAULT_MAX_HISTORY):
        self.workflow_id = workflow_id
        self.max_history = max_history
        self._history_length = len(history)
        self._last_seq = history[-1].seq if history else 0
        self._commands = [e for e in history if e.type in COMMAND_TYPES]
        self._results = {e.payload[field]: e for e in history if (field := RESULT_TYPES.get(e.type))}
        self._signals: dict[str, list[Event]] = defaultdict(list)
        for e in history:
            if e.type == "SignalReceived":
                self._signals[e.payload["name"]].append(e)
        self._signals_taken: Counter[str] = Counter()
        self._pos = 0
        self.new_commands: list[Command] = []
        self.nondeterminism: NonDeterminismError | None = None

    def _command(self, type: str, payload: dict, identity: tuple[str, ...], *, suspend: bool = True) -> Event | None:
        """Take the next command position and return the event history recorded there.

        If history is exhausted, record `payload` as a new command and suspend (or, with
        `suspend=False`, return None and keep running). If history holds a different
        command (type or `identity` fields differ), the code changed under a running
        workflow: raise NonDeterminismError (G5).
        """
        if self._pos >= len(self._commands):
            self._pos += 1
            self.new_commands.append(Command(type, payload))
            if suspend:
                raise _Suspend()
            return None
        recorded = self._commands[self._pos]
        expected = {k: recorded.payload.get(k) for k in identity}
        actual = {k: payload[k] for k in identity}
        if recorded.type != type or expected != actual:
            # Remembered on the context too, so workflow code that catches broad
            # exceptions cannot hide it.
            self.nondeterminism = NonDeterminismError(
                f"command {self._pos}: history has {recorded.type} "
                f"{ {k: recorded.payload.get(k) for k in ('name', 'args', 'seconds', 'kind') if k in recorded.payload} }, "
                f"code issued {type} {actual}"
            )
            raise self.nondeterminism
        self._pos += 1
        return recorded

    async def activity(self, name: str, args: list | tuple = (), *, retry: dict | None = None,
                       timeout: float | None = None, heartbeat_timeout: float | None = None) -> Any:
        """Run an activity at least once and return its result (or raise ActivityError).

        `timeout` limits one attempt. With `heartbeat_timeout`, an attempt that goes that
        long without calling `durably.heartbeat()` is treated as hung and retried.
        """
        recorded = self._command("ActivityScheduled", {
            "name": name, "args": _normalize(list(args)), "retry": retry, "timeout": timeout,
            "heartbeat_timeout": heartbeat_timeout,
        }, identity=("name", "args"))
        outcome = self._results.get(recorded.seq)
        if outcome is None:
            raise _Suspend()  # scheduled, still running
        if outcome.type == "ActivityFailed":
            raise ActivityError(name, outcome.payload["error"])
        return outcome.payload["result"]

    async def sleep(self, seconds: float) -> None:
        """Sleep durably: survives crashes, restarts and deploys.

        The wake-up time is fixed when the timer is first recorded, on the database clock.
        """
        recorded = self._command("TimerStarted", {"seconds": float(seconds), "for_signal": None},
                                 identity=("seconds", "for_signal"))
        if recorded.seq not in self._results:
            raise _Suspend()

    def _marker(self, kind: str, make) -> Any:
        """Return the value recorded for this marker, or record a fresh one from `make()`."""
        recorded = self._command("MarkerRecorded", {"kind": kind, "value": None}, identity=("kind",), suspend=False)
        if recorded is not None:
            return recorded.payload["value"]
        value = _normalize(make())
        self.new_commands[-1].payload["value"] = value
        return value

    def now(self) -> datetime:
        """The current UTC time, recorded the first time so every replay sees the same value."""
        return datetime.fromisoformat(self._marker("now", lambda: datetime.now(timezone.utc).isoformat()))

    def random(self) -> float:
        """A random float in [0, 1), recorded so every replay sees the same value."""
        return self._marker("random", random.random)

    def uuid4(self) -> UUID:
        """A random UUID, recorded so every replay sees the same value."""
        return UUID(self._marker("uuid4", lambda: str(uuid.uuid4())))

    def should_continue_as_new(self) -> bool:
        """True once this run's history has reached `max_history` events.

        Check it at a safe point (for example once per loop iteration) and call
        `continue_as_new` when it is True, so replay time stays bounded. A True answer is
        recorded as a marker, so replays answer exactly as the original run did.
        """
        nxt = self._commands[self._pos] if self._pos < len(self._commands) else None
        if nxt is not None:  # replaying: True only where the original run recorded it
            if nxt.type == "MarkerRecorded" and nxt.payload.get("kind") == "continue_as_new_suggested":
                self._pos += 1
                return True
            return False
        if self._history_length + len(self.new_commands) < self.max_history:
            return False
        self._command("MarkerRecorded", {"kind": "continue_as_new_suggested", "value": True},
                      identity=("kind",), suspend=False)
        return True

    def continue_as_new(self, input: Any) -> NoReturn:
        """End this run and start a fresh one with `input` and an empty history.

        Signals this run received but did not take are carried over to the new run, in
        order. Clients holding the original id follow the chain automatically.
        """
        raise _ContinueAsNew(_normalize(input))

    def _untaken_signals(self) -> list[dict]:
        untaken = [e for name, events in self._signals.items() for e in events[self._signals_taken[name]:]]
        return [e.payload for e in sorted(untaken, key=lambda e: e.seq)]

    async def wait_signal(self, name: str, timeout: float | None = None) -> Any:
        """Wait for the next signal called `name` and return its payload.

        Signals are taken in the order they were sent, each exactly once (G8): the n-th
        wait for `name` gets the n-th signal, on every replay. With `timeout`, returns None
        if no signal arrives in time; a later signal then goes to the next wait.
        """
        n = self._signals_taken[name]
        signals = self._signals[name]
        signal = signals[n] if n < len(signals) else None

        # A timed wait that found no signal yet started a timer. On replay, tell that apart
        # from a wait that got its signal straight away: the timer is the next command and
        # the signal (if any) arrived after it.
        nxt = self._commands[self._pos] if self._pos < len(self._commands) else None
        timer_was_started = (
            timeout is not None and nxt is not None and nxt.type == "TimerStarted"
            and nxt.payload.get("for_signal") == name and (signal is None or signal.seq > nxt.seq)
        )
        if not timer_was_started:
            if signal is not None:
                self._signals_taken[name] += 1
                return signal.payload["payload"]
            if timeout is None:
                raise _Suspend()  # nothing to schedule; the signal itself wakes the workflow
        timer = self._command("TimerStarted", {"seconds": float(timeout), "for_signal": name},
                              identity=("seconds", "for_signal"))
        fired = self._results.get(timer.seq)
        if signal is not None and (fired is None or signal.seq < fired.seq):
            self._signals_taken[name] += 1
            return signal.payload["payload"]
        if fired is not None:
            return None  # timed out; this signal (if it comes) is left for the next wait
        raise _Suspend()


# Every outcome but NonDeterministic carries the new commands to append, in order.
@dataclass
class Completed:
    result: Any
    commands: list[Command] = field(default_factory=list)


@dataclass
class Failed:
    error: str
    commands: list[Command] = field(default_factory=list)


@dataclass
class Suspended:
    commands: list[Command] = field(default_factory=list)  # empty: waiting on something already recorded


@dataclass
class ContinuedAsNew:
    input: Any
    signals: list[dict]  # untaken signal payloads to carry over, in order
    seen_through: int  # last seq this replay saw; later signals are carried too
    commands: list[Command] = field(default_factory=list)


@dataclass
class NonDeterministic:
    error: str


async def replay(fn, ctx: Context, input: Any) -> Completed | Failed | Suspended | ContinuedAsNew | NonDeterministic:
    """Run the workflow function against history once and report how it stopped."""
    try:
        result = await fn(ctx, input)
    except _Suspend:
        outcome = Suspended(ctx.new_commands)
    except _ContinueAsNew as e:
        outcome = ContinuedAsNew(e.input, ctx._untaken_signals(), ctx._last_seq, ctx.new_commands)
    except Exception as e:
        outcome = Failed(f"{type(e).__name__}: {e}", ctx.new_commands)
    else:
        outcome = None
    if ctx.nondeterminism is not None:
        return NonDeterministic(str(ctx.nondeterminism))
    if outcome is not None:
        return outcome
    try:
        return Completed(_normalize(result), ctx.new_commands)
    except (TypeError, ValueError) as e:
        return Failed(f"workflow result is not JSON-serializable: {e}", ctx.new_commands)
