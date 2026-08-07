import asyncio

import pytest

from ingest.core.ratelimit import TokenBucket, bucket_for


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def test_burst_up_to_capacity_then_refuses():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_sec=1.0, capacity=3.0, now=clock)
    assert [bucket.try_acquire() for _ in range(3)] == [True, True, True]
    assert bucket.try_acquire() is False


def test_refills_at_the_configured_rate():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_sec=2.0, capacity=2.0, now=clock)
    bucket.try_acquire(2.0)
    assert bucket.try_acquire() is False
    clock.advance(0.5)  # 0.5s * 2/s = 1 token
    assert bucket.try_acquire() is True


def test_refill_never_exceeds_capacity():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_sec=10.0, capacity=2.0, now=clock)
    clock.advance(100.0)
    assert bucket.try_acquire(2.0) is True
    assert bucket.try_acquire() is False


async def test_acquire_waits_instead_of_failing():
    bucket = TokenBucket(rate_per_sec=100.0, capacity=1.0)
    await bucket.acquire()
    await asyncio.wait_for(bucket.acquire(), timeout=1.0)


def test_known_venues_have_conservative_limits():
    assert bucket_for("kraken").rate_per_sec <= 1.0
    assert bucket_for("coinbase").rate_per_sec <= 10.0
    with pytest.raises(KeyError):
        bucket_for("nasdaq-direct-feed")
