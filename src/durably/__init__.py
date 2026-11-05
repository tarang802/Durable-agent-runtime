"""durably: crash-safe AI agents on plain Postgres (educational implementation)."""

from durably.client import Client
from durably.context import Context
from durably.errors import ActivityError, NonDeterminismError, StaleWorkerError, WorkflowFailedError
from durably.registry import activity, workflow

__version__ = "0.1.0.dev0"


def __getattr__(name: str):
    # Imported lazily so `python -m durably.worker` does not load the module twice.
    if name in ("ActivityWorker", "WorkflowWorker"):
        from durably import worker
        return getattr(worker, name)
    raise AttributeError(f"module 'durably' has no attribute {name!r}")

__all__ = [
    "ActivityError", "ActivityWorker", "Client", "Context", "NonDeterminismError",
    "StaleWorkerError", "WorkflowFailedError", "WorkflowWorker", "activity", "workflow",
]
