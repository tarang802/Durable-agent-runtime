"""Helpers shared by the benchmark scripts."""

import asyncio
import os
import platform
import statistics
import sys
import time

from durably.db import connect, init_db


async def fresh_schema() -> None:
    conn = await connect()
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await init_db(conn)
    await conn.close()


async def environment() -> str:
    conn = await connect()
    version = await conn.fetchval("SHOW server_version")
    await conn.close()
    return f"Python {platform.python_version()}, PostgreSQL {version}, {os.cpu_count()} CPUs ({platform.machine()})"


async def spawn_worker(kind: str, *, lease: float = 30.0, failpoint: str | None = None) -> asyncio.subprocess.Process:
    """A `durably worker` process running `experiments.app`."""
    env = {k: v for k, v in os.environ.items() if k != "FAILPOINT"}
    if failpoint:
        env["FAILPOINT"] = failpoint
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", "durably.worker", "--kind", kind, "--module", "experiments.app",
        "--lease", str(lease), "--poll", "0.02", env=env, stderr=asyncio.subprocess.DEVNULL,
    )


async def stop(*procs: asyncio.subprocess.Process) -> None:
    for p in procs:
        if p.returncode is None:
            p.kill()
            await p.wait()


async def timed(fn, repeats: int = 5) -> float:
    """Median wall time of `await fn()` in milliseconds."""
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        await fn()
        samples.append((time.perf_counter() - start) * 1000)
    return statistics.median(samples)


def table(title: str, headers: list[str], rows: list[list]) -> str:
    lines = [f"### {title}", "", "| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)
