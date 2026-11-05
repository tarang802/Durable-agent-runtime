# durably

**Durable execution for AI agents, with just Postgres.**

Your agent crashes after a paid LLM call. durably resumes it on another worker
without repeating the call — and without running a separate workflow server.

> **Early release (0.x):** the core guarantees are tested; the API may change
> before 1.0. See the [changelog](CHANGELOG.md).

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

## Usage

Activities do the real work. Each one receives a stable `idempotency_key`
(`workflow_id:activity_seq`) to pass to the provider:

```python
import durably

@durably.activity("llm")
async def llm(prompt, *, idempotency_key):
    return await my_llm_client.complete(prompt, idempotency_key=idempotency_key)

@durably.activity("send_email")
async def send_email(to, body, *, idempotency_key):
    await my_email_client.send(to, body, idempotency_key=idempotency_key)
```

A workflow is plain async code that calls activities through `ctx`:

```python
@durably.workflow("research_agent")
async def research_agent(ctx, input):
    plan = await ctx.activity("llm", [f"Plan research on {input['topic']}"])
    summary = await ctx.activity("llm", [f"Summarize: {plan}"], retry={"max_attempts": 5}, timeout=60)
    await ctx.activity("send_email", [input["email"], summary])
    return {"summary": summary}
```

Run workers, then start workflows from your app:

```bash
durably init-db
durably worker --kind workflow --module my_app.workflows
durably worker --kind activity --module my_app.activities
```

```python
client = await durably.Client.connect()
wf_id = await client.start(research_agent, {"topic": "durable execution", "email": "me@example.com"},
                           idempotency_key="research:42")
print(await client.result(wf_id, timeout=300))
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

## When to use durably vs. Temporal

| | durably | Temporal |
|---|---|---|
| Infrastructure | The Postgres you already run | A Temporal server cluster (or Temporal Cloud) |
| Languages | Python | Go, Java, TypeScript, Python, .NET, PHP |
| Scale | One Postgres; thousands of workflows | Very large fleets, sharded history |
| Workflow versioning | Detects changed code; `ctx.version` planned | Mature versioning and worker build IDs |
| Setup | `pip install durably` + `durably init-db` | Deploy and operate a server |

Choose durably when you want crash-safe agents inside an existing Python + Postgres
backend without another service to run. Choose Temporal when you need multi-language
workflows or scale beyond a single database.

## Known limitations

- Activities in one workflow run one at a time; parallel activities (`asyncio.gather`)
  are planned.
- Timers, signals, heartbeats, `ctx.now()`/`ctx.random()` and Continue-As-New are in
  progress.
- No schema migrations yet: upgrading between 0.x releases may require re-creating tables.
- Side effects are at-least-once; make activities idempotent with the key provided.

## Stack

Python 3.12+, asyncio and asyncpg (the only runtime dependency), PostgreSQL 16. Tests use
pytest, Hypothesis, Toxiproxy and Docker Compose.

## Development

Tests run in a Linux container next to Postgres 16 and Toxiproxy, so they behave the same
on every machine (crash tests rely on POSIX signals). You only need Docker.

```bash
docker compose run --rm --build tests
```

To create the schema in your own Postgres:

```bash
pip install -e .
durably init-db --database-url postgres://user:pass@host:5432/db
```

## License

MIT
