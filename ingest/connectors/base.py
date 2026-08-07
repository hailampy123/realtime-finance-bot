from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from ingest.core.gaps import Gap
from ingest.core.models import Trade


@runtime_checkable
class Connector(Protocol):
    """One venue's wire format and nothing else.

    Resilience, rate limiting, queueing, and publishing all live in core/;
    a connector only knows how to address a venue and read its frames.
    """

    venue: str

    def stream_url(self, symbols: list[str]) -> str: ...

    def subscribe_payloads(self, symbols: list[str]) -> list[dict[str, Any]]: ...

    def parse(self, raw: str) -> list[Trade]: ...

    async def repair(self, gap: Gap) -> list[Trade]: ...
