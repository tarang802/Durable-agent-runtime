"""A research agent: plan, search, summarize with a paid LLM, wait for approval, email.

The providers below are stand-ins so the example runs without API keys. Swap them for
your LLM, search and email clients, and pass `idempotency_key` through to them so a
retried call is not billed or sent twice.
"""

import asyncio

import durably


async def call_llm(prompt: str, idempotency_key: str) -> str:
    await asyncio.sleep(0.3)  # pretend to be a slow, paid API
    return f"<LLM answer to: {prompt[:60]}>"


@durably.activity("llm")
async def llm(prompt, *, idempotency_key):
    print(f"  llm        paid call   key={idempotency_key}")
    return await call_llm(prompt, idempotency_key)


@durably.activity("search")
async def search(query, *, idempotency_key):
    print(f"  search     {query!r}")
    await asyncio.sleep(0.1)
    return [f"result for {query} #{i}" for i in range(2)]


@durably.activity("send_email")
async def send_email(to, body, *, idempotency_key):
    print(f"  send_email to={to} key={idempotency_key}")
    return "sent"


@durably.workflow("research_agent")
async def research_agent(ctx, input):
    topic = input["topic"]
    plan = await ctx.activity("llm", [f"Plan research on {topic}"])

    results = []
    for angle in ("history", "state of the art", "open problems"):
        results += await ctx.activity("search", [f"{topic} {angle}"], retry={"max_attempts": 5})

    summary = await ctx.activity("llm", [f"Summarize for a busy reader: {plan} {results}"], timeout=60)

    # A person reviews the summary. The workflow waits durably: restarts and deploys
    # while it waits lose nothing.
    decision = await ctx.wait_signal("approval", timeout=24 * 3600)
    if not decision or not decision.get("approved"):
        return {"status": "not sent", "summary": summary}

    await ctx.activity("send_email", [input["email"], summary])
    return {"status": "sent", "summary": summary, "approved_by": decision.get("by")}
