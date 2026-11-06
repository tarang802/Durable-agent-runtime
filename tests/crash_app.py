"""Workflow and activities that crash tests run in separate worker processes.

Loaded by `python -m durably.worker --module tests.crash_app`; not imported by pytest.
"""

import asyncio
import os

import durably
from durably.testing import SharedFakeLLM

# The fake provider is "outside" the system under test, so it may bypass a proxy that
# sits in front of durably's own database connection.
llm = SharedFakeLLM(os.environ.get("FAKE_LLM_URL"))


@durably.activity("shared_llm")
async def shared_llm(prompt, *, idempotency_key):
    return await llm.complete(prompt, idempotency_key=idempotency_key)


@durably.workflow("research")
async def research(ctx, topic):
    plan = await ctx.activity("shared_llm", [f"plan {topic}"])
    summary = await ctx.activity("shared_llm", [f"summarize {plan}"])
    return summary


@durably.workflow("nap")
async def nap(ctx, topic):
    await ctx.sleep(0.5)
    return await ctx.activity("shared_llm", [f"after nap {topic}"])


@durably.activity("slow_llm")
async def slow_llm(prompt, *, idempotency_key):
    await asyncio.sleep(0.5)
    return await llm.complete(prompt, idempotency_key=idempotency_key)


@durably.workflow("slow_research")
async def slow_research(ctx, topic):
    plan = await ctx.activity("slow_llm", [f"plan {topic}"])
    return await ctx.activity("slow_llm", [f"summarize {plan}"])
