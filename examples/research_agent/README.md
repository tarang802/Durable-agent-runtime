# Research agent example

Plans, runs three searches, summarizes with a paid LLM call, waits for a person to
approve, then sends an email. The providers in [`app.py`](app.py) are stand-ins, so it
runs without API keys.

## Run it in one process

From the repository root, with Docker:

```bash
docker compose run --rm tests python -m examples.research_agent.run
```

## Run it like production

Start Postgres (`docker compose up -d postgres`, exposed on port 5433), install durably,
and run workers as separate processes:

```bash
pip install -e .
durably init-db
durably worker --kind workflow --module examples.research_agent.app
durably worker --kind activity --module examples.research_agent.app
```

Then start a workflow and approve it from Python:

```python
import asyncio, durably

async def main():
    client = await durably.Client.connect()
    wf = await client.start("research_agent", {"topic": "durable execution", "email": "me@example.com"},
                            idempotency_key="research-1")
    input("Press Enter once the summary is ready to approve it...")
    await client.signal(wf, "approval", {"approved": True, "by": "me"})
    print(await client.result(wf, timeout=60))
    await client.close()

asyncio.run(main())
```

Kill the activity worker in the middle of the run (or restart both workers) and start it
again: the workflow continues from its last completed step, and the paid LLM calls that
already finished are not made again.
