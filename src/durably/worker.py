"""Workflow and activity workers.

Run with `durably worker --kind workflow|activity --module my_app.workflows`
(or `python -m durably.worker ...`).
"""

import argparse
import asyncio
import importlib
import logging
import os
import socket
import uuid

import asyncpg

from durably import activity_context, registry, retry, store
from durably.activity_context import ActivityInfo
from durably.context import (
    DEFAULT_MAX_HISTORY, Completed, Context, ContinuedAsNew, Failed, NonDeterministic, Suspended, replay,
)
from durably.db import create_pool
from durably.errors import StaleWorkerError
from durably.failpoints import failpoint

log = logging.getLogger("durably.worker")

NONDETERMINISM_RETRY_S = 5.0
MAX_ERROR_BACKOFF_S = 2.0


def _worker_id(kind: str) -> str:
    return f"{kind}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"


class _Worker:
    kind = ""

    def __init__(self, pool: asyncpg.Pool, *, lease: float = 30.0, poll: float = 0.2, worker_id: str | None = None):
        self.pool = pool
        self.lease = lease
        self.poll = poll
        self.id = worker_id or _worker_id(self.kind)

    async def run_once(self) -> bool:
        """Claim and process one task. Returns False if nothing was due."""
        raise NotImplementedError

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        errors = 0
        while not stop.is_set():
            try:
                did_work = await self.run_once()
                errors = 0
            except Exception as e:
                # Typically the database is unreachable. Whatever this task did is rolled
                # back or left to lease expiry; back off instead of hammering the database.
                errors += 1
                if errors == 1:
                    log.exception("%s: task processing failed", self.id)
                else:
                    log.warning("%s: still failing (%d in a row): %s", self.id, errors, e)
                did_work = False
            if not did_work:
                delay = min(self.poll * 2 ** errors, MAX_ERROR_BACKOFF_S) if errors else self.poll
                try:
                    await asyncio.wait_for(stop.wait(), delay)
                except TimeoutError:
                    pass


class WorkflowWorker(_Worker):
    kind = "workflow"

    def __init__(self, pool: asyncpg.Pool, *, max_history: int = DEFAULT_MAX_HISTORY, **kwargs):
        super().__init__(pool, **kwargs)
        self.max_history = max_history

    async def run_once(self) -> bool:
        # Workflow workers also act as the timer poller.
        async with self.pool.acquire() as conn:
            if await store.fire_due_timer(conn):
                return True
        failpoint("before_claim")
        async with self.pool.acquire() as conn:
            claim = await store.claim_workflow_task(conn, self.id, self.lease)
            if claim is None:
                return False
            failpoint("after_claim")
            wf = await conn.fetchrow("SELECT type, input, status FROM workflows WHERE id = $1", claim.workflow_id)
            history = await store.load_history(conn, claim.workflow_id)

            fn = registry.get_workflow(wf["type"])
            outcome: Completed | Failed | Suspended | ContinuedAsNew | NonDeterministic | None
            if wf["status"] != "running":
                outcome = None
            elif fn is None:
                outcome = NonDeterministic(f"workflow type {wf['type']!r} is not registered in this worker")
            else:
                outcome = await replay(fn, Context(claim.workflow_id, history, self.max_history), wf["input"])

            failpoint("before_append")
            try:
                async with conn.transaction():
                    await self._apply(conn, claim, outcome, wf["type"])
            except StaleWorkerError as e:
                # Another worker took over after our lease expired. Our whole transaction
                # rolled back, so history is untouched; just walk away.
                log.warning("%s: abandoning task %s: %s", self.id, claim.task_id, e)
                return True
            failpoint("after_record_before_ack")
        return True

    async def _apply(self, conn, claim, outcome, wf_type: str) -> None:
        wf_id, epoch = claim.workflow_id, claim.epoch
        await store.take_own_task(conn, claim, self.id)  # first: tasks before workflows
        for cmd in getattr(outcome, "commands", []):
            seq = await store.append(conn, wf_id, epoch, cmd.type, cmd.payload)
            if cmd.type == "TimerStarted":
                await store.enqueue_timer(conn, wf_id, seq, cmd.payload["seconds"])
            elif cmd.type == "ActivityScheduled":
                # Stable idempotency key: the seq of the ActivityScheduled event never
                # changes, however many times the activity is retried.
                await store.enqueue_activity(conn, wf_id, {
                    **cmd.payload, "scheduled_seq": seq, "idempotency_key": f"{wf_id}:{seq}",
                })
        match outcome:
            case Suspended() | None:
                pass
            case Completed(result=result):
                await store.append(conn, wf_id, epoch, "WorkflowCompleted", {"result": result})
                await conn.execute("UPDATE workflows SET status = 'completed', result = $2 WHERE id = $1", wf_id, result)
            case ContinuedAsNew(input=new_input, signals=signals, seen_through=seen_through):
                await store.append(conn, wf_id, epoch, "WorkflowContinuedAsNew", {"input": new_input})
                # The append holds this run's row lock, so no signal can land after this
                # query; ones that landed after our replay loaded history are carried too.
                late = await conn.fetch(
                    "SELECT payload FROM events WHERE workflow_id = $1 AND seq > $2 AND type = 'SignalReceived' ORDER BY seq",
                    wf_id, seen_through,
                )
                carried = [("SignalReceived", s) for s in signals + [r["payload"] for r in late]]
                new_run = await store.create_workflow(conn, wf_type, new_input, started={"continued_from": str(wf_id)},
                                                      carried=carried)
                await conn.execute("UPDATE workflows SET status = 'continued_as_new', continued_as = $2 WHERE id = $1",
                                   wf_id, new_run)
            case Failed(error=error):
                await store.append(conn, wf_id, epoch, "WorkflowFailed", {"error": error})
                await conn.execute("UPDATE workflows SET status = 'failed', error = $2 WHERE id = $1", wf_id, error)
            case NonDeterministic(error=error):
                # No event is written, so history stays exactly as it was. The
                # workflow keeps running and retries once someone fixes the code.
                log.error("%s: workflow %s: %s", self.id, wf_id, error)
                await store.wake_workflow(conn, wf_id, NONDETERMINISM_RETRY_S)  # tasks before workflows
                await conn.execute("UPDATE workflows SET error = $2 WHERE id = $1", wf_id, f"NonDeterminismError: {error}")
        await store.release_workflow(conn, claim)


class ActivityWorker(_Worker):
    kind = "activity"

    async def run_once(self) -> bool:
        failpoint("before_claim")
        async with self.pool.acquire() as conn:
            claim = await store.claim_activity_task(conn, self.id, self.lease)
        if claim is None:
            return False
        failpoint("after_claim")
        p = claim.payload
        policy = retry.policy(p.get("retry"))
        fn = registry.get_activity(p["name"])
        try:
            if fn is None:
                raise LookupError(f"activity {p['name']!r} is not registered in this worker")
            failpoint("before_activity")
            info = ActivityInfo(str(claim.workflow_id), p["name"], p["idempotency_key"], claim.attempt,
                                p.get("heartbeat_details"))
            token = activity_context._current.set(activity_context._Running(
                info, self.pool, claim, self.id, p.get("heartbeat_timeout") or self.lease))
            try:
                call = fn(*p["args"], idempotency_key=p["idempotency_key"])
                result = await asyncio.wait_for(call, p["timeout"]) if p.get("timeout") else await call
            finally:
                activity_context._current.reset(token)
            failpoint("after_effect_before_record")
            payload = {"scheduled_seq": p["scheduled_seq"], "result": result}
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            async with self.pool.acquire() as conn:
                if claim.attempt < policy["max_attempts"]:
                    await store.retry_activity(conn, claim, self.id, retry.delay(policy, claim.attempt))
                else:
                    await store.record_activity_result(conn, claim, self.id, "ActivityFailed", {
                        "scheduled_seq": p["scheduled_seq"], "error": error, "attempts": claim.attempt,
                    })
            log.info("%s: activity %s attempt %d failed: %s", self.id, p["name"], claim.attempt, error)
            return True

        async with self.pool.acquire() as conn:
            if not await store.record_activity_result(conn, claim, self.id, "ActivityCompleted", payload):
                log.warning("%s: dropped stale result for task %s", self.id, claim.task_id)
                return True
        failpoint("after_record_before_ack")
        return True


WORKERS = {"workflow": WorkflowWorker, "activity": ActivityWorker}


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--kind", choices=sorted(WORKERS), required=True)
    parser.add_argument("--module", action="append", default=[], help="module that registers workflows/activities (repeatable)")
    parser.add_argument("--database-url", default=None, help="defaults to $DATABASE_URL")
    parser.add_argument("--lease", type=float, default=30.0, help="lease length in seconds")
    parser.add_argument("--poll", type=float, default=0.2, help="idle poll interval in seconds")
    parser.add_argument("--max-history", type=int, default=DEFAULT_MAX_HISTORY,
                        help="history length at which ctx.should_continue_as_new() turns True (workflow workers)")


async def serve(args: argparse.Namespace) -> None:
    for module in args.module:
        importlib.import_module(module)
    pool = await create_pool(args.database_url)
    extra = {"max_history": args.max_history} if args.kind == "workflow" else {}
    worker = WORKERS[args.kind](pool, lease=args.lease, poll=args.poll, **extra)
    log.info("%s: started", worker.id)
    try:
        await worker.run()
    finally:
        await pool.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="durably.worker")
    add_arguments(parser)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(serve(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
