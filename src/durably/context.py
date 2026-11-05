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

COMMAND_TYPES = {"ActivityScheduled"}
RESULT_TYPES = {"ActivityCompleted", "ActivityFailed"}


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
        self._results = {e.payload["scheduled_seq"]: e for e in history if e.type in RESULT_TYPES}
        self._pos = 0
        self.new_command: Command | None = None
        self.nondeterminism: NonDeterminismError | None = None

    async def activity(self, name: str, args: list | tuple = (), *, retry: dict | None = None,
                       timeout: float | None = None) -> Any:
        """Run an activity at least once and return its result (or raise ActivityError)."""
        args = _normalize(list(args))
        if self._pos < len(self._commands):
            recorded = self._commands[self._pos]
            if (recorded.type, recorded.payload["name"], recorded.payload["args"]) != ("ActivityScheduled", name, args):
                # Remembered on the context too, so workflow code that catches broad
                # exceptions cannot hide it (G5).
                self.nondeterminism = NonDeterminismError(
                    f"command {self._pos}: history has {recorded.type} "
                    f"{recorded.payload.get('name')!r}{recorded.payload.get('args')}, "
                    f"code called activity {name!r}{args}"
                )
                raise self.nondeterminism
            self._pos += 1
            outcome = self._results.get(recorded.seq)
            if outcome is None:
                raise _Suspend()  # scheduled, still running
            if outcome.type == "ActivityFailed":
                raise ActivityError(name, outcome.payload["error"])
            return outcome.payload["result"]

        self._pos += 1
        self.new_command = Command("ActivityScheduled", {"name": name, "args": args, "retry": retry, "timeout": timeout})
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
