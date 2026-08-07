from __future__ import annotations

import json
import time
from typing import Any

import httpx
import structlog

from ingest.core.gaps import Gap
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Side, Source, Trade
from ingest.core.ratelimit import bucket_for

log = structlog.get_logger(__name__)

WS_BASE = "wss://stream.binance.com:9443/stream?streams="
REST_AGG_TRADES = "https://api.binance.com/api/v3/aggTrades"
MAX_REPAIR_BATCH = 1000


class BinanceConnector:
    """Binance aggregate-trade stream.

    We subscribe to @aggTrade rather than @trade because the aggregate id `a`
    shares an id space with the keyless /api/v3/aggTrades?fromId= endpoint,
    which makes exact-range gap repair possible without an API key. The raw
    @trade stream would need /api/v3/historicalTrades, which requires one.
    """

    venue = "binance"
    # aggTrade ids are persistent across reconnects; resetting here would mask real gaps.
    resets_sequence_on_reconnect = False

    def __init__(self, instruments: InstrumentMap) -> None:
        self._instruments = instruments

    def stream_url(self, symbols: list[str]) -> str:
        streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in symbols)
        return f"{WS_BASE}{streams}"

    def subscribe_payloads(self, symbols: list[str]) -> list[dict[str, Any]]:
        # Streams are named in the URL; there is no subscribe frame.
        return []

    def parse(self, raw: str) -> list[Trade]:
        frame = json.loads(raw)
        data = frame.get("data", frame)
        if data.get("e") != "aggTrade":
            return []
        return [t for t in (self._to_trade(data),) if t is not None]

    def _to_trade(
        self,
        data: dict[str, Any],
        *,
        symbol: str | None = None,
        source: Source = Source.STREAM,
    ) -> Trade | None:
        venue_symbol = symbol or data["s"]
        try:
            instrument_id = self._instruments.canonical(self.venue, venue_symbol)
        except KeyError:
            log.warning("unmapped_symbol", venue=self.venue, symbol=venue_symbol)
            return None
        return Trade(
            venue=self.venue,
            venue_symbol=venue_symbol,
            instrument_id=instrument_id,
            trade_id=str(data["a"]),
            event_ts_us=int(data["T"]) * 1000,
            ingest_ts_us=time.time_ns() // 1000,
            price=str(data["p"]),
            size=str(data["q"]),
            # m=True means the buyer was the maker, so the seller crossed the spread.
            side=Side.SELL if data["m"] else Side.BUY,
            sequence=int(data["a"]),
            is_backfill=source is not Source.STREAM,
            source=source,
        )

    async def repair(self, gap: Gap) -> list[Trade]:
        """Re-fetch the missing aggregate-trade id range over REST."""
        await bucket_for(self.venue).acquire()
        params: dict[str, str | int] = {
            "symbol": gap.venue_symbol,
            "fromId": gap.last_seen + 1,
            "limit": min(MAX_REPAIR_BATCH, max(gap.missing_count, 1)),
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(REST_AGG_TRADES, params=params)
            response.raise_for_status()
            rows = response.json()

        trades: list[Trade] = []
        for row in rows:
            try:
                if int(row["a"]) >= gap.next_seen:
                    continue  # the stream already delivered these
                trade = self._to_trade(row, symbol=gap.venue_symbol, source=Source.REST_REPAIR)
            except (KeyError, ValueError) as exc:
                log.warning("malformed_repair_row", venue=self.venue, error=str(exc), row=row)
                continue
            if trade is not None:
                trades.append(trade)
        log.info(
            "gap_repaired",
            venue=self.venue,
            symbol=gap.venue_symbol,
            requested=gap.missing_count,
            recovered=len(trades),
        )
        return trades
