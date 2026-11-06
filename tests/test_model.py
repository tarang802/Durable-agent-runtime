"""Model-based test: random interleavings checked against a simple reference model.

Hypothesis drives the engine one step at a time: start workflows (sometimes with a
repeated idempotency key), send signals, run one workflow task or one activity task, or
crash a worker mid-task (claim and abandon it, or run an activity's effect and die before
recording it). After every step the database must agree with the model, and once
everything is drained each workflow must have produced the result the model predicts.

On failure Hypothesis prints the minimal failing steps and a `@reproduce_failure` line;
`pytest tests/test_model.py --hypothesis-seed=<n>` reruns a whole session deterministically.
"""

import asyncio
import os
from collections import Counter

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, rule

import durably
from durably import store
from durably.db import connect, create_pool, init_db
from durably.registry import get_activity

effects: Counter[str] = Counter()  # times_ten calls per idempotency key


@durably.activity("times_ten")
async def times_ten(v, *, idempotency_key):
    effects[idempotency_key] += 1
    return v * 10


@durably.workflow("collector")
async def collector(ctx, input):
    out = []
    for _ in range(input["want"]):
        v = await ctx.wait_signal("v")
        out.append(await ctx.activity("times_ten", [v]))
        await ctx.sleep(0)
    return out


async def _fresh_pool():
    conn = await connect()
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await init_db(conn)
    await conn.close()
    return await create_pool(min_size=1, max_size=4)


class DurablyMachine(RuleBasedStateMachine):
    workflows = Bundle("workflows")

    def __init__(self):
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.pool = self.run(_fresh_pool())
        self.client = durably.Client(self.pool)
        self.wf_worker = durably.WorkflowWorker(self.pool, lease=30, poll=0)
        self.act_worker = durably.ActivityWorker(self.pool, lease=30, poll=0)
        self.model: dict[str, dict] = {}  # workflow id -> {"want": n, "sent": [values]}
        effects.clear()

    def run(self, coro):
        return self.loop.run_until_complete(coro)

    # --- steps -------------------------------------------------------------------

    @rule(target=workflows, want=st.integers(1, 3), key=st.sampled_from([None, None, "k1", "k2"]))
    def start(self, want, key):
        wf = self.run(self.client.start(collector, {"want": want}, idempotency_key=key))
        self.model.setdefault(wf, {"want": want, "sent": []})  # a repeated key keeps the first input
        return wf

    @rule(wf=workflows, value=st.integers(0, 9))
    def signal(self, wf, value):
        try:
            self.run(self.client.signal(wf, "v", value))
            self.model[wf]["sent"].append(value)
        except durably.WorkflowNotRunningError:
            m = self.model[wf]
            assert len(m["sent"]) >= m["want"], "workflow finished before receiving enough signals"

    @rule()
    def workflow_step(self):
        self.run(self.wf_worker.run_once())

    @rule()
    def activity_step(self):
        self.run(self.act_worker.run_once())

    @rule()
    def workflow_worker_crashes_after_claim(self):
        # Claim with a zero lease and walk away: the next claim takes it over at once.
        async def crash():
            async with self.pool.acquire() as conn:
                await store.claim_workflow_task(conn, "crashed-worker", 0)
        self.run(crash())

    @rule()
    def activity_worker_crashes_after_effect(self):
        # The effect happens, then the worker dies before recording it.
        async def crash():
            async with self.pool.acquire() as conn:
                claim = await store.claim_activity_task(conn, "crashed-worker", 0)
            if claim is not None:
                p = claim.payload
                await get_activity(p["name"])(*p["args"], idempotency_key=p["idempotency_key"])
        self.run(crash())

    # --- checks after every step -------------------------------------------------

    @invariant()
    def history_agrees_with_model(self):
        async def check():
            async with self.pool.acquire() as conn:
                for wf, m in self.model.items():
                    history = await store.load_history(conn, wf)
                    types = [e.type for e in history]
                    # G1: seqs are 1..n with no gaps or repeats.
                    assert [e.seq for e in history] == list(range(1, len(history) + 1))
                    # G4: at most one terminal event.
                    assert types.count("WorkflowCompleted") + types.count("WorkflowFailed") <= 1
                    # Each scheduled activity has at most one recorded outcome.
                    outcomes = Counter(e.payload["scheduled_seq"] for e in history
                                       if e.type in ("ActivityCompleted", "ActivityFailed"))
                    assert all(n == 1 for n in outcomes.values())
                    # G8: signals are recorded in send order.
                    assert [e.payload["payload"] for e in history if e.type == "SignalReceived"] == m["sent"]
                    row = await conn.fetchrow("SELECT status, result FROM workflows WHERE id = $1", wf)
                    if row["status"] == "completed":
                        assert row["result"] == [v * 10 for v in m["sent"][:m["want"]]]
                    else:
                        assert row["status"] == "running"
        self.run(check())

    def teardown(self):
        try:
            async def drain():
                idle = 0
                while idle < 3:  # a few idle rounds: crashed claims and zero timers are due at once
                    did = await self.wf_worker.run_once() | await self.act_worker.run_once()
                    idle = 0 if did else idle + 1

            self.run(asyncio.wait_for(drain(), 30))
            self.history_agrees_with_model()
            for wf, m in self.model.items():
                status = self.run(self.client.describe(wf))["status"]
                # Liveness: enough signals means the workflow finished; otherwise it waits.
                assert status == ("completed" if len(m["sent"]) >= m["want"] else "running"), (wf, m, status)
        finally:
            self.run(self.pool.close())
            self.loop.close()


DurablyMachine.TestCase.settings = settings(
    max_examples=int(os.environ.get("DURABLY_MODEL_EXAMPLES", "40")),
    stateful_step_count=30,
    deadline=None,
    database=None,
    print_blob=True,
    suppress_health_check=[HealthCheck.too_slow],
)
TestDurablyModel = DurablyMachine.TestCase
