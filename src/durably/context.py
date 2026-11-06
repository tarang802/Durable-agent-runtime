"""The replay engine.

A workflow task reruns the workflow function from the top against its history. Each
`ctx.*` call takes the next command position:

- history has a command there: it must match the call (else NonDeterminismError); if
  its result is recorded, return it without running anything (G3);
- history is exhausted: this is a new command; record it and suspend.

Suspending raises `_Suspend`, which unwinds the function. Nothing stays parked in memory:
the next event wakes a fresh replay.
"""

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from durably.errors import ActivityError, NonDeterminismError
from durably.store import Event

COMMAND_TYPES = {"ActivityScheduled", "TimerStarted"}
# Result event type -> payload field naming the command it answers.
RESULT_TYPES = {"ActivityCompleted": "scheduled_seq", "ActivityFailed": "scheduled_seq", "TimerFired": "started_seq"}


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


class Context:
    def __init__(self, workflow_id: UUID, history: list[Event]):
        self.workflow_id = workflow_id
        self._commands = [e for e in history if e.type in COMMAND_TYPES]
        self._results = {e.payload[field]: e for e in history if (field := RESULT_TYPES.get(e.type))}
        self._pos = 0
        self.new_command: Command | None = None
        self.nondeterminism: NonDeterminismError | None = None

    def _command(self, type: str, payload: dict, identity: tuple[str, ...]) -> Event:
        """Take the next command position and return the event history recorded there.

        If history is exhausted, record `payload` as a new command and suspend. If history
        holds a different command (type or `identity` fields differ), the code changed
        under a running workflow: raise NonDeterminismError (G5).
        """
        if self._pos >= len(self._commands):
            self._pos += 1
            self.new_command = Command(type, payload)
            raise _Suspend()
        recorded = self._commands[self._pos]
        expected = {k: recorded.payload.get(k) for k in identity}
        actual = {k: payload[k] for k in identity}
        if recorded.type != type or expected != actual:
            # Remembered on the context too, so workflow code that catches broad
            # exceptions cannot hide it.
            self.nondeterminism = NonDeterminismError(
                f"command {self._pos}: history has {recorded.type} "
                f"{ {k: recorded.payload.get(k) for k in ('name', 'args', 'seconds') if k in recorded.payload} }, "
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
        recorded = self._command("TimerStarted", {"seconds": float(seconds)}, identity=("seconds",))
        if recorded.seq not in self._results:
            raise _Suspend()


@dataclass
class Completed:
    result: Any


@dataclass
class Failed:
    error: str


@dataclass
class Suspended:
    command: Command | None  # None: waiting on something already scheduled


@dataclass
class NonDeterministic:
    error: str


async def replay(fn, ctx: Context, input: Any) -> Completed | Failed | Suspended | NonDeterministic:
    """Run the workflow function against history once and report how it stopped."""
    try:
        result = await fn(ctx, input)
    except _Suspend:
        outcome = Suspended(ctx.new_command)
    except Exception as e:
        outcome = Failed(f"{type(e).__name__}: {e}")
    else:
        outcome = None
    if ctx.nondeterminism is not None:
        return NonDeterministic(str(ctx.nondeterminism))
    if outcome is not None:
        return outcome
    try:
        return Completed(_normalize(result))
    except (TypeError, ValueError) as e:
        return Failed(f"workflow result is not JSON-serializable: {e}")
