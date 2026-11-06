"""Client API: start workflows and wait for their results."""

import asyncio
from collections.abc import Callable
from typing import Any

import asyncpg

from durably import registry, store
from durably.db import create_pool
from durably.errors import WorkflowFailedError, WorkflowNotRunningError


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

        A second start with the same idempotency key returns the existing workflow's
        id and starts nothing, even if `input` differs.
        """
        async with self.pool.acquire() as conn, conn.transaction():
            wf_id = await store.create_workflow(conn, registry.name_of(workflow), input, idempotency_key=idempotency_key)
            if wf_id is None:
                return str(await conn.fetchval("SELECT id FROM workflows WHERE idempotency_key = $1", idempotency_key))
        return str(wf_id)

    async def signal(self, workflow_id: str, name: str, payload: Any = None) -> None:
        """Send a signal to a running workflow; `ctx.wait_signal(name)` receives it.

        The signal is recorded in the workflow's history before this returns, so it is not
        lost if no worker is running. If the workflow continued as new, the signal goes to
        its latest run. Raises WorkflowNotRunningError if the workflow has finished or does
        not exist.
        """
        async with self.pool.acquire() as conn:
            while True:
                run = await store.latest_run(conn, workflow_id)
                if run is None or run["status"] != "running":
                    raise WorkflowNotRunningError(f"workflow {workflow_id} is not running")
                tx = conn.transaction()
                await tx.start()
                if await store.deliver(conn, run["id"], "SignalReceived", {"name": name, "payload": payload}):
                    await tx.commit()
                    return
                # The run finished or continued as new in the meantime: roll back the wake-up
                # and look up the latest run again.
                await tx.rollback()

    async def describe(self, workflow_id: str) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, type, status, epoch, result, error, continued_as, created_at FROM workflows WHERE id = $1",
                workflow_id,
            )
        if row is None:
            raise LookupError(f"no workflow {workflow_id}")
        return dict(row)

    async def history(self, workflow_id: str) -> list[store.Event]:
        async with self.pool.acquire() as conn:
            return await store.load_history(conn, workflow_id)

    async def runs(self, workflow_id: str) -> list[str]:
        """Ids of every run of a workflow, oldest first (more than one after Continue-As-New)."""
        async with self.pool.acquire() as conn:
            ids, current = [], workflow_id
            while current is not None:
                ids.append(str(current))
                current = await conn.fetchval("SELECT continued_as FROM workflows WHERE id = $1", current)
        return ids

    async def result(self, workflow_id: str, timeout: float | None = None, poll: float = 0.05) -> Any:
        """Wait for the workflow to finish and return its result.

        Follows Continue-As-New to the final run. Raises WorkflowFailedError if it failed,
        TimeoutError if `timeout` passes first.
        """
        async def wait() -> Any:
            while True:
                async with self.pool.acquire() as conn:
                    run = await store.latest_run(conn, workflow_id)
                if run is None:
                    raise LookupError(f"no workflow {workflow_id}")
                if run["status"] == "completed":
                    return run["result"]
                if run["status"] in ("failed", "cancelled"):
                    raise WorkflowFailedError(f"workflow {workflow_id} {run['status']}: {run['error']}")
                await asyncio.sleep(poll)

        return await asyncio.wait_for(wait(), timeout)
