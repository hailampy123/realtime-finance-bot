"""Sequence-gap detection.

A WebSocket that silently misses forty seconds of trades is the classic
market-data failure, and it is invisible unless something is explicitly
watching the sequence numbers.
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class Gap:
    venue: str
    venue_symbol: str
    last_seen: int
    next_seen: int

    @property
    def missing_count(self) -> int:
        return self.next_seen - self.last_seen - 1


class SequenceTracker:
    def __init__(self) -> None:
        self._watermarks: dict[tuple[str, str], int] = {}

    def observe(self, venue: str, venue_symbol: str, sequence: int | None) -> Gap | None:
        if sequence is None:
            return None
        key = (venue, venue_symbol)
        last = self._watermarks.get(key)
        self._watermarks[key] = max(sequence, last) if last is not None else sequence
        if last is None or sequence <= last + 1:
            return None
        gap = Gap(venue=venue, venue_symbol=venue_symbol, last_seen=last, next_seen=sequence)
        log.warning(
            "gap_detected",
            venue=venue,
            symbol=venue_symbol,
            missing=gap.missing_count,
            last_seen=last,
            next_seen=sequence,
        )
        return gap

    def reset(self, venue: str, venue_symbol: str) -> None:
        self._watermarks.pop((venue, venue_symbol), None)
