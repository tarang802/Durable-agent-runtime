"""`@workflow` and `@activity` decorators and the name -> function registry.

Workers look functions up by name, so a workflow started by one process can be run by
another that imported the same module.
"""

from collections.abc import Callable

_workflows: dict[str, Callable] = {}
_activities: dict[str, Callable] = {}


def _register(table: dict[str, Callable], name: str | Callable | None):
    def decorate(fn: Callable) -> Callable:
        key = name if isinstance(name, str) else fn.__name__
        fn.__durably_name__ = key
        table[key] = fn
        return fn

    if callable(name):  # used bare: @workflow
        return decorate(name)
    return decorate


def workflow(name: str | Callable | None = None):
    """Register `async def fn(ctx, input)` as a workflow."""
    return _register(_workflows, name)


def activity(name: str | Callable | None = None):
    """Register `async def fn(*args, idempotency_key)` as an activity."""
    return _register(_activities, name)


def name_of(fn_or_name: Callable | str) -> str:
    return fn_or_name if isinstance(fn_or_name, str) else fn_or_name.__durably_name__


def get_workflow(name: str) -> Callable | None:
    return _workflows.get(name)


def get_activity(name: str) -> Callable | None:
    return _activities.get(name)
