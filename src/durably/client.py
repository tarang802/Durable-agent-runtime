"""Client API: start workflows and wait for their results."""

import asyncio
from collections.abc import Callable
from typing import Any

import asyncpg

from durably import registry, store
from durably.db import create_pool
from durably.errors import WorkflowFailedError


class Client:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self._owns_pool = False

    @classmethod
    async def connect(cls, url: str | None = None) -> "Client":
        client = cls(await create_pool(url))
        client._owns_pool = True
        return client

    async def close(self) -> None:
        if self._owns_pool:
            await self.pool.close()

    async def start(self, workflow: Callable | str, input: Any = None, idempotency_key: str | None = None) -> str:
        """Start a workflow and return its id.

        FR-1: a second start with the same idempotency key returns the existing
        workflow's id and starts nothing, even if `input` differs.
        """
        async with self.pool.acquire() as conn, conn.transaction():
            wf_id = await conn.fetchval(
                """
                INSERT INTO workflows (type, input, idempotency_key, next_seq) VALUES ($1, $2, $3, 2)
                ON CONFLICT (idempotency_key) DO NOTHING RETURNING id
                """,
                registry.name_of(workflow), input, idempotency_key,
            )
            if wf_id is None:
                return str(await conn.fetchval("SELECT id FROM workflows WHERE idempotency_key = $1", idempotency_key))
            # The row is brand new and unclaimed, so seq 1 is ours without fencing.
            await conn.execute(
                "INSERT INTO events (workflow_id, seq, type, payload) VALUES ($1, 1, 'WorkflowStarted', $2)",
                wf_id, {"input": input},
            )
            await store.wake_workflow(conn, wf_id)
        return str(wf_id)

    async def describe(self, workflow_id: str) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, type, status, epoch, result, error, created_at FROM workflows WHERE id = $1",
                workflow_id,
            )
        if row is None:
            raise LookupError(f"no workflow {workflow_id}")
        return dict(row)

    async def history(self, workflow_id: str) -> list[store.Event]:
        async with self.pool.acquire() as conn:
            return await store.load_history(conn, workflow_id)

    async def result(self, workflow_id: str, timeout: float | None = None, poll: float = 0.05) -> Any:
        """Wait for the workflow to finish and return its result.

        Raises WorkflowFailedError if it failed, TimeoutError if `timeout` passes first.
        """
        async def wait() -> Any:
            while True:
                wf = await self.describe(workflow_id)
                if wf["status"] == "completed":
                    return wf["result"]
                if wf["status"] in ("failed", "cancelled"):
                    raise WorkflowFailedError(f"workflow {workflow_id} {wf['status']}: {wf['error']}")
                await asyncio.sleep(poll)

        return await asyncio.wait_for(wait(), timeout)
