"""Workflow and activities that crash tests run in separate worker processes.

Loaded by `python -m durably.worker --module tests.crash_app`; not imported by pytest.
"""

import durably
from durably.testing import SharedFakeLLM

llm = SharedFakeLLM()


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
