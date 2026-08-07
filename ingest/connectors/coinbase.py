from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

import httpx
import structlog

from ingest.core.gaps import Gap
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Side, Source, Trade
from ingest.core.ratelimit import bucket_for

log = structlog.get_logger(__name__)

WS_URL = "wss://advanced-trade-ws.coinbase.com"
REST_TICKER = "https://api.coinbase.com/api/v3/brokerage/market/products/{product_id}/ticker"
REPAIR_LIMIT = 100


class CoinbaseConnector:
    """Coinbase Advanced Trade market_trades channel.

    Two things differ from Binance and both are load-bearing:

    * `sequence_num` is a connection-wide counter, not per product, so gaps are
      tracked under the sentinel symbol "*".
    * The public market-data API has no id-range query, so repair is
      best-effort: refetch the most recent trades per product and let
      natural-key dedupe in Silver absorb the overlap. Not every venue supports
      the same repair, and pretending otherwise would hide a real limitation.
    """

    venue = "coinbase"
    sequence_symbol = "*"

    def __init__(self, instruments: InstrumentMap) -> None:
        self._instruments = instruments

    def stream_url(self, symbols: list[str]) -> str:
        return WS_URL

    def subscribe_payloads(self, symbols: list[str]) -> list[dict[str, Any]]:
        return [{"type": "subscribe", "channel": "market_trades", "product_ids": symbols}]

    def parse(self, raw: str) -> list[Trade]:
        frame = json.loads(raw)
        if frame.get("channel") != "market_trades":
            return []
        sequence = frame.get("sequence_num")
        trades: list[Trade] = []
        for event in frame.get("events", []):
            for row in event.get("trades", []):
                try:
                    trade = self._to_trade(row, sequence=sequence, source=Source.STREAM)
                except (KeyError, ValueError) as exc:
                    log.warning("malformed_trade", venue=self.venue, error=str(exc), row=row)
                    continue
                if trade is not None:
                    trades.append(trade)
        return trades

    def _to_trade(
        self, row: dict[str, Any], *, sequence: int | None, source: Source
    ) -> Trade | None:
        venue_symbol = row["product_id"]
        try:
            instrument_id = self._instruments.canonical(self.venue, venue_symbol)
        except KeyError:
            log.warning("unmapped_symbol", venue=self.venue, symbol=venue_symbol)
            return None
        return Trade(
            venue=self.venue,
            venue_symbol=venue_symbol,
            instrument_id=instrument_id,
            trade_id=str(row["trade_id"]),
            event_ts_us=_rfc3339_to_us(row["time"]),
            ingest_ts_us=time.time_ns() // 1000,
            price=str(row["price"]),
            size=str(row["size"]),
            side=Side(row["side"]) if row.get("side") in ("BUY", "SELL") else Side.UNKNOWN,
            sequence=int(sequence) if sequence is not None else None,
            is_backfill=source is not Source.STREAM,
            source=source,
        )

    async def repair(self, gap: Gap) -> list[Trade]:
        products = (
            self._instruments.symbols_for(self.venue)
            if gap.venue_symbol == self.sequence_symbol
            else [gap.venue_symbol]
        )
        recovered: list[Trade] = []
        async with httpx.AsyncClient(timeout=10.0) as client:
            for product_id in products:
                await bucket_for(self.venue).acquire()
                response = await client.get(
                    REST_TICKER.format(product_id=product_id), params={"limit": REPAIR_LIMIT}
                )
                response.raise_for_status()
                for row in response.json().get("trades", []):
                    row.setdefault("product_id", product_id)
                    trade = self._to_trade(row, sequence=None, source=Source.REST_REPAIR)
                    if trade is not None:
                        recovered.append(trade)
        log.info("gap_repaired", venue=self.venue, products=len(products), recovered=len(recovered))
        return recovered


def _rfc3339_to_us(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1_000_000)
