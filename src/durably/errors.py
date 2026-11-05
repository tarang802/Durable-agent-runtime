"""Exceptions raised by durably."""


class DurablyError(Exception):
    pass


class StaleWorkerError(DurablyError):
    """This worker's epoch is no longer current; another worker owns the workflow."""


class NonDeterminismError(DurablyError):
    """Workflow code issued a different command than history recorded at this position."""


class ActivityError(DurablyError):
    """An activity failed after exhausting its retries. Raised inside the workflow."""

    def __init__(self, name: str, message: str):
        super().__init__(f"activity {name!r} failed: {message}")
        self.name = name
        self.message = message


class WorkflowFailedError(DurablyError):
    """Raised by `Client.result` when the workflow ended in failure."""
