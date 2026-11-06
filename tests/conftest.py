import asyncio
import json
import os
import signal
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

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
                    lease: float = 1.0, database_url: str | None = None) -> WorkerProcess:
        env = {k: v for k, v in os.environ.items() if k not in ("FAILPOINT", "DURABLY_FENCING")}
        if database_url:
            env["FAKE_LLM_URL"] = env["DATABASE_URL"]
            env["DATABASE_URL"] = database_url
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


class PostgresProxy:
    """A Toxiproxy proxy in front of Postgres, for cutting workers' connections."""

    def __init__(self, api: str, url: str):
        self.api = api
        self.url = url  # connect through here instead of DATABASE_URL

    def _call(self, method: str, path: str, body: dict | None = None) -> None:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.api + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=5).close()
        except urllib.error.HTTPError as e:
            if e.code != 409:  # 409: the proxy already exists
                raise

    async def set_enabled(self, enabled: bool) -> None:
        await asyncio.to_thread(self._call, "POST", "/proxies/postgres", {"enabled": enabled})

    async def cut(self, seconds: float) -> None:
        """Drop every open connection and refuse new ones for `seconds`."""
        await self.set_enabled(False)
        await asyncio.sleep(seconds)
        await self.set_enabled(True)


@pytest.fixture
async def pg_proxy(db):
    direct = urlparse(os.environ["DATABASE_URL"])
    proxy = PostgresProxy(os.environ["TOXIPROXY_URL"], direct._replace(netloc=f"{direct.username}:{direct.password}@toxiproxy:6432").geturl())
    await asyncio.to_thread(proxy._call, "POST", "/proxies", {
        "name": "postgres", "listen": "0.0.0.0:6432", "upstream": f"{direct.hostname}:{direct.port}", "enabled": True,
    })
    await proxy.set_enabled(True)
    yield proxy
    await proxy.set_enabled(True)
