"""Workflows and activities used by the benchmarks (worker processes load this too)."""

import durably
from durably.testing import SharedFakeLLM

llm = SharedFakeLLM()


@durably.activity("noop")
async def noop(*args, idempotency_key):
    return None


@durably.activity("paid_llm")
async def paid_llm(prompt, *, idempotency_key):
    return await llm.complete(prompt, idempotency_key=idempotency_key)


@durably.workflow("steps")
async def steps(ctx, n):
    for i in range(n):
        await ctx.activity("noop", [i])
    return n


@durably.workflow("agent")
async def agent(ctx, n):
    """An agent that makes `n` paid LLM calls in a row."""
    answer = "start"
    for i in range(n):
        answer = await ctx.activity("paid_llm", [f"step {i}: {answer}"])
    return answer
