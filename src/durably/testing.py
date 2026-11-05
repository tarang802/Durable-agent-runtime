"""Test helpers: a fake LLM provider that counts calls and side effects."""

import asyncio
from collections import Counter


class FakeLLMError(Exception):
    pass


class FakeLLM:
    """A stand-in for a paid LLM API.

    - `calls` counts every request, `calls_by_key` per idempotency key.
    - `effects_by_key` counts billable effects: like a real provider honouring an
      Idempotency-Key header, a repeated key returns the first response for free (G6).
    - `fail_next(n)` makes the next n requests raise; `latency` delays every request.
    """

    def __init__(self, latency: float = 0.0):
        self.latency = latency
        self.reset()

    def reset(self) -> None:
        self.calls = 0
        self.calls_by_key: Counter[str] = Counter()
        self.effects_by_key: Counter[str] = Counter()
        self._responses: dict[str, str] = {}
        self._failures = 0

    def fail_next(self, n: int) -> None:
        self._failures = n

    async def complete(self, prompt: str, *, idempotency_key: str) -> str:
        self.calls += 1
        self.calls_by_key[idempotency_key] += 1
        if self.latency:
            await asyncio.sleep(self.latency)
        if self._failures > 0:
            self._failures -= 1
            raise FakeLLMError("injected failure")
        if idempotency_key not in self._responses:
            self.effects_by_key[idempotency_key] += 1
            self._responses[idempotency_key] = f"llm({prompt})"
        return self._responses[idempotency_key]
