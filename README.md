# durable-agent-runtime

**Crash-safe AI agents on plain Postgres.**

> **Status: alpha (0.x), an educational implementation.**
> Built to show how durable execution works, and small enough to read in an afternoon.
>
> ✅ Good for: learning, side projects, internal tools, prototypes
> ❌ Not for: mission-critical production. Use Temporal, Inngest or Restate.

## What it is

`durably` is a small workflow engine for long-running agent loops. A workflow is an
ordinary async Python function that calls activities (an LLM call, a tool call). The
engine records every step in an append-only history in PostgreSQL. If a worker dies,
another worker reruns the function against that history: recorded steps return their
saved result instead of running again, and execution continues from the first
unrecorded step.

**Example.** A research agent plans, runs three searches, summarizes with a paid LLM call,
then sends an email. The worker is killed after the summary. A naive restart repeats the
paid call and may send the email twice. With `durably`, the restart skips everything
recorded and sends the email once.

## What it will look like

```python
import durably

@durably.workflow("research_agent")
async def research_agent(ctx, input):
    plan = await ctx.activity("llm", [f"Plan research on {input['topic']}"])
    results = [await ctx.activity("search", [q]) for q in plan.splitlines()[:3]]
    summary = await ctx.activity("llm", [f"Summarize: {results}"])
    await ctx.activity("send_email", [input["email"], summary])
    return {"summary": summary}
```

```bash
durably worker --kind workflow --module my_app.workflows
durably worker --kind activity --module my_app.activities
```

## Guarantees (planned, one test each)

| | Guarantee |
|---|---|
| G1 | History is append-only and `(workflow_id, seq)` is unique |
| G2 | A write from a stale epoch is rejected |
| G3 | Replay never re-runs an activity recorded as completed |
| G4 | At most one `WorkflowCompleted` event per workflow |
| G5 | A changed workflow body is detected as non-determinism |
| G6 | The fake provider's effect count per idempotency key is exactly one |

Side effects run **at least once**, with a stable idempotency key
(`workflow_id:activity_seq`). Exactly-once is impossible in general, so activities should
be idempotent.

## Stack

Python 3.12, asyncio, asyncpg, PostgreSQL 16 (the only stateful dependency), pytest,
Hypothesis, Toxiproxy, Docker Compose. No Temporal, Celery or queue library.

## Development

Tests run in a Linux container next to Postgres 16 and Toxiproxy ([ADR 0003](docs/adr/0003-tests-run-in-linux-container.md)).
You only need Docker.

```bash
docker compose run --rm --build tests
```

To create the schema in your own Postgres:

```bash
pip install -e .
durably init-db --database-url postgres://user:pass@host:5432/db
```

## Architecture decisions

| ADR | Decision |
|---|---|
| [0001](docs/adr/0001-postgres-only.md) | PostgreSQL is the only stateful dependency |
| [0002](docs/adr/0002-package-naming.md) | Package, import and CLI are named `durably` |
| [0003](docs/adr/0003-tests-run-in-linux-container.md) | Tests run in a Linux container |
| [0004](docs/adr/0004-append-only-enforced-by-trigger.md) | The database enforces append-only history |

## Roadmap

See [PLAN.md](PLAN.md) for the phase-by-phase build plan.

1. Schema, `start`, one worker running two activities in order
2. Replay and resume after a crash
3. Epoch fencing against stale workers
4. Retries, timeouts, heartbeats, timers, signals
5. Non-determinism detection, `now()`/`random()` markers, Continue-As-New
6. Failpoint matrix, Hypothesis model test, benchmark scripts

## License

MIT
