"""Test helpers: fake LLM providers that count calls and side effects."""

import asyncio
from collections import Counter
from dataclasses import dataclass

import asyncpg

from durably.db import connect


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


@dataclass
class FakeLLMStats:
    calls_by_key: Counter[str]
    calls_by_prompt: Counter[str]
    effects_by_prompt: Counter[str]  # billable effects; >1 means a step was paid for twice

    @property
    def duplicate_calls(self) -> int:
        return sum(self.calls_by_key.values()) - len(self.calls_by_key)


class SharedFakeLLM:
    """A FakeLLM whose counters live in Postgres.

    Worker processes share it and its counts survive a worker being killed, which is
    what crash tests need. Call `install(conn)` once to create its tables.
    """

    def __init__(self, url: str | None = None):
        self.url = url

    @staticmethod
    async def install(conn: asyncpg.Connection) -> None:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fake_llm_calls (
                id bigserial PRIMARY KEY, idempotency_key text NOT NULL, prompt text NOT NULL);
            CREATE TABLE IF NOT EXISTS fake_llm_effects (
                idempotency_key text PRIMARY KEY, prompt text NOT NULL, response text NOT NULL);
            """
        )

    async def complete(self, prompt: str, *, idempotency_key: str) -> str:
        conn = await connect(self.url)
        try:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO fake_llm_calls (idempotency_key, prompt) VALUES ($1, $2)", idempotency_key, prompt
                )
                # A repeated key returns the first response without a new effect.
                await conn.execute(
                    """
                    INSERT INTO fake_llm_effects (idempotency_key, prompt, response) VALUES ($1, $2, $3)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    """,
                    idempotency_key, prompt, f"llm({prompt})",
                )
                return await conn.fetchval(
                    "SELECT response FROM fake_llm_effects WHERE idempotency_key = $1", idempotency_key
                )
        finally:
            await conn.close()

    async def stats(self) -> FakeLLMStats:
        conn = await connect(self.url)
        try:
            calls = await conn.fetch("SELECT idempotency_key, prompt FROM fake_llm_calls")
            effects = await conn.fetch("SELECT prompt FROM fake_llm_effects")
        finally:
            await conn.close()
        return FakeLLMStats(
            calls_by_key=Counter(r["idempotency_key"] for r in calls),
            calls_by_prompt=Counter(r["prompt"] for r in calls),
            effects_by_prompt=Counter(r["prompt"] for r in effects),
        )
