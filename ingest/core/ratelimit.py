"""Per-venue token buckets.

REST is only used for gap repair and snapshots, so the budget is small — but
exceeding a venue's published limit gets the source IP banned, which on a
weekly-rebuilt sandbox is a genuinely painful failure mode.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from functools import lru_cache


class TokenBucket:
    def __init__(
        self,
        rate_per_sec: float,
        capacity: float,
        *,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self.rate_per_sec = rate_per_sec
        self.capacity = capacity
        self._now = now
        self._tokens = capacity
        self._updated = now()

    def _refill(self) -> None:
        current = self._now()
        elapsed = current - self._updated
        self._updated = current
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_sec)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0) -> None:
        while not self.try_acquire(tokens):
            self._refill()
            deficit = tokens - self._tokens
            await asyncio.sleep(max(deficit / self.rate_per_sec, 0.001))


# Published public-endpoint limits, deliberately set below the documented ceiling.
_RATES: dict[str, tuple[float, float]] = {
    "binance": (10.0, 20.0),
    "coinbase": (8.0, 10.0),
    "kraken": (1.0, 1.0),
}


@lru_cache(maxsize=16)
def bucket_for(venue: str) -> TokenBucket:
    if venue not in _RATES:
        raise KeyError(f"no rate limit configured for venue {venue!r}")
    rate, capacity = _RATES[venue]
    return TokenBucket(rate_per_sec=rate, capacity=capacity)
