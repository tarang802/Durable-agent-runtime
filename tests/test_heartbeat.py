"""Heartbeats keep long activities alive and let hung ones be retried."""

import asyncio

import pytest

import durably

attempts: list[int] = []


@pytest.fixture(autouse=True)
def reset():
    attempts.clear()


@durably.activity("slow_but_alive")
async def slow_but_alive(*, idempotency_key):
    attempts.append(durably.activity_info().attempt)
    for _ in range(10):  # 1 s of work, well past the 0.3 s heartbeat timeout
        await asyncio.sleep(0.1)
        await durably.heartbeat()
    return "done"


@durably.activity("hangs_once")
async def hangs_once(*, idempotency_key):
    info = durably.activity_info()
    attempts.append(info.attempt)
    if info.attempt == 1:
        await durably.heartbeat({"processed": 3})
        await asyncio.sleep(1.5)  # hung: no more heartbeats
        return "first attempt (should be dropped)"
    return {"resumed_from": info.heartbeat_details}


@durably.workflow("heartbeat_long")
async def heartbeat_long(ctx, input):
    return await ctx.activity("slow_but_alive", heartbeat_timeout=0.3)


@durably.workflow("heartbeat_hung")
async def heartbeat_hung(ctx, input):
    return await ctx.activity("hangs_once", heartbeat_timeout=0.3, retry={"initial_interval": 0.01})


async def test_heartbeats_keep_a_long_activity_on_one_attempt(client, workers):
    workers(lease=0.3, activity_workers=2)
    wf = await client.start(heartbeat_long)

    assert await client.result(wf, timeout=10) == "done"
    assert attempts == [1]  # never reclaimed by the second worker


async def test_hung_activity_is_retried_with_last_heartbeat_details(client, workers):
    workers(activity_workers=2)
    wf = await client.start(heartbeat_hung)

    assert await client.result(wf, timeout=10) == {"resumed_from": {"processed": 3}}
    assert attempts == [1, 2]


def test_activity_info_outside_an_activity_raises():
    with pytest.raises(RuntimeError, match="inside a running activity"):
        durably.activity_info()
