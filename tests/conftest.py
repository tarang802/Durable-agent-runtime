import asyncio
import os
import signal
import sys
from pathlib import Path

import pytest

from durably import ActivityWorker, Client, WorkflowWorker
from durably.db import connect, create_pool, init_db
from durably.testing import SharedFakeLLM


@pytest.fixture
async def db():
    """A connection to a freshly reset schema, so every test starts empty."""
    conn = await connect()
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await init_db(conn)
    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
async def pool(db):
    p = await create_pool(min_size=1, max_size=10)
    try:
        yield p
    finally:
        await p.close()


@pytest.fixture
async def client(pool):
    return Client(pool)


@pytest.fixture
async def workers(pool):
    """Call to start one workflow worker and one activity worker in this process."""
    stop = asyncio.Event()
    tasks = []

    def start(lease: float = 5.0, activity_workers: int = 1, max_history: int = 1000):
        workers = [WorkflowWorker(pool, lease=lease, poll=0.01, max_history=max_history)]
        workers += [ActivityWorker(pool, lease=lease, poll=0.01) for _ in range(activity_workers)]
        tasks.extend(asyncio.create_task(w.run(stop)) for w in workers)

    yield start
    stop.set()
    await asyncio.gather(*tasks)


@pytest.fixture
async def shared_llm(db):
    await SharedFakeLLM.install(db)
    return SharedFakeLLM()


class WorkerProcess:
    """A `durably worker` subprocess running `tests.crash_app`."""

    def __init__(self, proc: asyncio.subprocess.Process):
        self.proc = proc

    @property
    def pid(self) -> int:
        return self.proc.pid

    async def exit_code(self, timeout: float = 15) -> int:
        return await asyncio.wait_for(self.proc.wait(), timeout)

    def is_stopped(self) -> bool:
        stat = Path(f"/proc/{self.pid}/stat").read_text()
        return stat.rsplit(")", 1)[1].split()[0] == "T"

    async def wait_stopped(self, timeout: float = 15) -> None:
        async def poll():
            while not self.is_stopped():
                await asyncio.sleep(0.02)
        await asyncio.wait_for(poll(), timeout)

    def resume(self) -> None:
        self.proc.send_signal(signal.SIGCONT)


@pytest.fixture
async def spawn(db):
    """Start worker processes; all are killed at the end of the test."""
    procs: list[WorkerProcess] = []

    async def start(kind: str, *, failpoint: str | None = None, fencing: bool = True,
                    lease: float = 1.0) -> WorkerProcess:
        env = {k: v for k, v in os.environ.items() if k not in ("FAILPOINT", "DURABLY_FENCING")}
        if failpoint:
            env["FAILPOINT"] = failpoint
        if not fencing:
            env["DURABLY_FENCING"] = "off"
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "durably.worker", "--kind", kind, "--module", "tests.crash_app",
            "--lease", str(lease), "--poll", "0.05", env=env,
        )
        procs.append(WorkerProcess(proc))
        return procs[-1]

    yield start
    for p in procs:
        if p.proc.returncode is None:
            p.proc.kill()  # SIGKILL also ends a stopped process
            await p.proc.wait()
