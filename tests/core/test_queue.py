import asyncio

import pytest

from ingest.core.queue import TOPIC_POLICIES, BoundedTopicQueue, DropPolicy


async def test_drop_oldest_evicts_and_counts():
    q = BoundedTopicQueue(maxsize=2, policy=DropPolicy.DROP_OLDEST)
    await q.put("a")
    await q.put("b")
    await q.put("c")
    assert q.qsize() == 2
    assert q.dropped == 1
    assert await q.get() == "b"
    assert await q.get() == "c"


async def test_block_policy_waits_for_room_and_never_drops():
    q = BoundedTopicQueue(maxsize=1, policy=DropPolicy.BLOCK)
    await q.put("a")
    put_task = asyncio.create_task(q.put("b"))
    await asyncio.sleep(0)
    assert not put_task.done(), "BLOCK must not complete while the queue is full"
    assert await q.get() == "a"
    await asyncio.wait_for(put_task, timeout=1.0)
    assert await q.get() == "b"
    assert q.dropped == 0


def test_trades_block_and_depth_drops():
    """The one failure this system must not have is silent trade loss."""
    assert TOPIC_POLICIES["md.trades.v1"] is DropPolicy.BLOCK
    assert TOPIC_POLICIES["md.book.depth.v1"] is DropPolicy.DROP_OLDEST


def test_every_configured_topic_has_an_explicit_policy():
    for topic, policy in TOPIC_POLICIES.items():
        assert isinstance(policy, DropPolicy), topic


async def test_get_waits_when_empty():
    q = BoundedTopicQueue(maxsize=1, policy=DropPolicy.BLOCK)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(q.get(), timeout=0.05)
