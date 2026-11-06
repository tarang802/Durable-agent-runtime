# durably

[![CI](https://github.com/tarang802/Durable-agent-runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/tarang802/Durable-agent-runtime/actions/workflows/ci.yml)

**Durable execution for AI agents, with just Postgres.**

Your agent crashes after a paid LLM call. durably resumes it on another worker
without repeating the call — and without running a separate workflow server.

> **Early release (0.x):** the core guarantees are tested; the API may change
> before 1.0. See the [changelog](https://github.com/tarang802/Durable-agent-runtime/blob/main/CHANGELOG.md).

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

## Install

Requires Python 3.12+ and PostgreSQL. CI tests against PostgreSQL 16.

```bash
pip install durably
```

Then create durably's tables in your database (safe to run again):

```bash
durably init-db --database-url postgres://user:pass@host:5432/db
```

Every command and `durably.Client.connect()` read `DATABASE_URL` when no URL is given.

To see it work first, run the [research agent example](https://github.com/tarang802/Durable-agent-runtime/tree/main/examples/research_agent),
which plans, searches, summarizes, waits for approval and sends an email:

```bash
git clone https://github.com/tarang802/Durable-agent-runtime && cd Durable-agent-runtime
docker compose run --rm tests python -m examples.research_agent.run
```

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

Workflows can sleep for minutes or days and wait for outside events. Both survive
crashes and deploys:

```python
@durably.workflow("publish_with_approval")
async def publish_with_approval(ctx, draft_id):
    draft = await ctx.activity("llm", [f"Write post {draft_id}"])
    decision = await ctx.wait_signal("review", timeout=3 * 24 * 3600)  # None if nobody answers in 3 days
    if decision is None or not decision["approved"]:
        return "not published"
    await ctx.sleep(3600)  # publish an hour later
    await ctx.activity("publish", [draft])
    return "published"
```

```python
await client.signal(wf_id, "review", {"approved": True})
```

Long-running activities heartbeat so a hung attempt is detected and retried, resuming
from the last saved progress:

```python
@durably.activity("embed_documents")
async def embed_documents(doc_ids, *, idempotency_key):
    start = durably.activity_info().heartbeat_details or 0  # progress from a previous attempt
    for i in range(start, len(doc_ids)):
        await embed(doc_ids[i])
        await durably.heartbeat(i + 1)
```

```python
await ctx.activity("embed_documents", [doc_ids], heartbeat_timeout=30)
```

Use `ctx.now()`, `ctx.random()` and `ctx.uuid4()` instead of `datetime.now()`, `random`
and `uuid`: the first value is recorded, so replays see the same one.

Agents that run indefinitely (a chat assistant, a monitor) roll over to a fresh run before
their history grows large, so replay stays fast:

```python
@durably.workflow("assistant")
async def assistant(ctx, state):
    while True:
        message = await ctx.wait_signal("message")
        reply = await ctx.activity("llm", [state["memory"], message])
        state["memory"] = await ctx.activity("summarize", [state["memory"], message, reply])
        if ctx.should_continue_as_new():  # history reached --max-history events
            ctx.continue_as_new(state)  # unread messages carry over to the new run
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

## Guarantees

Each guarantee is checked by a test, most of them with real worker processes that are
killed or frozen mid-run.

| | Guarantee | Test |
|---|---|---|
| G1 | History is append-only and `(workflow_id, seq)` is unique | `test_events_are_append_only`, `test_event_seq_is_unique_per_workflow` |
| G2 | A write from a stale worker is rejected | `test_stale_worker_write_rejected` (paired with `test_without_fencing_stale_worker_corrupts_history`) |
| G3 | Replay never re-runs an activity recorded as completed | `test_resume_does_not_repeat_completed_activity` |
| G4 | At most one terminal event per workflow | `test_second_terminal_event_is_rejected` |
| G5 | A changed workflow body is detected as non-determinism | `test_changed_code_is_nondeterminism` |
| G6 | With an idempotent provider, every step is billed exactly once, whatever the crash point | `test_crash_matrix` |
| G7 | A durable timer fires exactly once, even if a worker dies while firing it | `test_timer_fires_once_after_crash_while_firing` |
| G8 | Each signal is received exactly once, in send order | `test_signals_are_taken_in_order_once_each`, `test_signals_sent_before_any_worker_runs_are_kept_in_order` |

### Crash matrix

A worker is killed at each failpoint, a replacement finishes the workflow, and the fake
provider counts calls:

| Worker | Killed at | Workflow finishes | Duplicate provider calls | Steps billed twice |
|---|---|---|---|---|
| activity | `before_claim` | ✅ | 0 | 0 |
| activity | `after_claim` | ✅ | 0 | 0 |
| activity | `before_activity` | ✅ | 0 | 0 |
| activity | `after_effect_before_record` | ✅ | 1 | 0 |
| activity | `after_record_before_ack` | ✅ | 0 | 0 |
| workflow | `before_claim` | ✅ | 0 | 0 |
| workflow | `after_claim` | ✅ | 0 | 0 |
| workflow | `before_append` | ✅ | 0 | 0 |
| workflow | `after_record_before_ack` | ✅ | 0 | 0 |

The one duplicate is unavoidable: the provider did the work but the worker died before
recording it, so the retry calls again. It carries the same idempotency key, so an
idempotent provider does not bill it twice.

Side effects run **at least once**, with a stable idempotency key
(`workflow_id:activity_seq`). Exactly-once is impossible in general, so activities should
be idempotent.

## Benchmarks

Generated by the scripts in [`experiments/`](https://github.com/tarang802/Durable-agent-runtime/tree/main/experiments) on a laptop (Docker Desktop on
Windows, 16 CPUs, Python 3.12, PostgreSQL 16). Full output: [RESULTS.md](https://github.com/tarang802/Durable-agent-runtime/blob/main/experiments/RESULTS.md).
Regenerate with:

```bash
docker compose run --rm tests python -m experiments.run_all
```

**Paid LLM calls when a 5-step agent's worker is killed after step k.** A naive restart
pays again for every completed step; durably pays for each step once.

| Killed after step | Naive restart: paid calls | durably: paid calls |
|---|---|---|
| 1 | 6 | 5 |
| 2 | 7 | 5 |
| 3 | 8 | 5 |
| 4 | 9 | 5 |

**Recovery time after a workflow worker dies**, from its claim to the workflow finishing on
a healthy worker. It tracks the lease length, so the lease is the knob.

| Lease (s) | Recovery (s) |
|---|---|
| 0.5 | 0.57 |
| 1 | 1.08 |
| 2 | 2.06 |
| 5 | 5.07 |

**Replay time vs history length** (load from Postgres plus replay). Continue-As-New keeps a
run at or below its limit, 1,000 events by default.

| Events in history | Load (ms) | Replay (ms) | Total (ms) |
|---|---|---|---|
| 99 | 0.7 | 0.3 | 1.0 |
| 999 | 3.3 | 2.0 | 5.4 |
| 4,999 | 17.6 | 10.2 | 27.8 |
| 9,999 | 32.2 | 21.5 | 53.7 |

**Activity throughput vs worker processes** (2,000 no-op activities, each a claim, run and
record). In this run two processes measured below one; throughput then grows with more
processes.

| Worker processes | Tasks per second |
|---|---|
| 1 | 177 |
| 2 | 151 |
| 4 | 296 |
| 8 | 530 |

## When to use durably vs. Temporal

| | durably | Temporal |
|---|---|---|
| Infrastructure | The Postgres you already run | A Temporal server cluster (or Temporal Cloud) |
| Languages | Python | Go, Java, TypeScript, Python, .NET, PHP |
| Scale | One Postgres; thousands of workflows | Very large fleets, sharded history |
| Workflow versioning | Detects changed code as non-determinism | Mature versioning and worker build IDs |
| Setup | `pip install` + `durably init-db` | Deploy and operate a server |

Choose durably when you want crash-safe agents inside an existing Python + Postgres
backend without another service to run. Choose Temporal when you need multi-language
workflows or scale beyond a single database.

## Known limitations

- Activities in one workflow run one at a time; `asyncio.gather` over activities is not
  supported.
- An activity that runs longer than the worker lease (30 s by default) without a
  `heartbeat_timeout` is treated as crashed and run again. Heartbeat long activities.
- Workflow code must make the same calls on every replay: use `ctx.now()`, `ctx.random()`
  and `ctx.sleep()` rather than the clock, `random` or `asyncio.sleep`, and do network I/O
  in activities.
- Replay reads a run's whole history, so workflows that loop indefinitely should use
  Continue-As-New.
- No schema migrations: upgrading between 0.x releases may require re-creating tables.
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

Crash tests use failpoints: `FAILPOINT=<name>` makes a worker exit abruptly at that point,
and `FAILPOINT=<name>:pause` freezes it with SIGSTOP. You can use them to crash-test your
own workflows the same way.

What the suite covers:

- **Crash recovery** with real worker processes killed or frozen at each failpoint.
- **Dropped connections:** Toxiproxy cuts the workers' Postgres connections mid-activity;
  they back off, reconnect and finish without billing a step twice.
- **Model-based testing:** a Hypothesis state machine runs random interleavings of
  starts, signals, worker steps and crashes, and checks history against a reference model
  after every step.

When the model test fails it prints the minimal failing steps and a
`@reproduce_failure(...)` line to paste above the test. To rerun a whole session
deterministically, pass a seed (more examples with `DURABLY_MODEL_EXAMPLES`):

```bash
docker compose run --rm tests pytest tests/test_model.py --hypothesis-seed=12345
```

To create the schema in your own Postgres:

```bash
pip install -e .
durably init-db --database-url postgres://user:pass@host:5432/db
```

## License

MIT
