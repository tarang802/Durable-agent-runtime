"""durably: durable execution for AI agents, with just Postgres."""

from durably.activity_context import ActivityInfo, activity_info, heartbeat
from durably.client import Client
from durably.context import Context
from durably.errors import (
    ActivityError, NonDeterminismError, StaleWorkerError, WorkflowFailedError, WorkflowNotRunningError,
)
from durably.registry import activity, workflow

__version__ = "0.1.0"


def __getattr__(name: str):
    # Imported lazily so `python -m durably.worker` does not load the module twice.
    if name in ("ActivityWorker", "WorkflowWorker"):
        from durably import worker
        return getattr(worker, name)
    raise AttributeError(f"module 'durably' has no attribute {name!r}")

__all__ = [
    "ActivityError", "ActivityInfo", "ActivityWorker", "Client", "Context", "NonDeterminismError",
    "StaleWorkerError", "WorkflowFailedError", "WorkflowNotRunningError", "WorkflowWorker",
    "activity", "activity_info",
    "heartbeat", "workflow",
]
