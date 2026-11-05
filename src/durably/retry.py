"""Activity retry policy: exponential backoff with jitter."""

import random

DEFAULT_POLICY = {
    "max_attempts": 3,
    "initial_interval": 1.0,  # seconds before the 2nd attempt
    "backoff": 2.0,
    "max_interval": 30.0,
}


def policy(overrides: dict | None) -> dict:
    return {**DEFAULT_POLICY, **(overrides or {})}


def delay(p: dict, attempt: int) -> float:
    """Seconds to wait after failed `attempt` (1-based).

    Jitter spreads retries of many failing activities so they do not hit a recovering
    provider at the same instant.
    """
    base = min(p["initial_interval"] * p["backoff"] ** (attempt - 1), p["max_interval"])
    return base * random.uniform(0.5, 1.0)
