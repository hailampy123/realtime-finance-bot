"""Normalised rows to gzipped CSV bytes for archive_staging. Pure.

WHY CSV AND NOT PARQUET (narrows spec §6.2). §6.2 says the loader writes Parquet.
Its actual requirement, stated in the same paragraph, is stronger and is what is
kept here: "Lambda does I/O only ... All merging is Athena SQL against a staging
external table. That keeps exactly one transform engine."

Writing Parquet from Lambda means pyarrow: ~90 MB of wheel, so a layer or a
container image, for a format that is read exactly once and then deleted. Gzipped
CSV needs nothing outside the standard library, so the loader ships as a plain zip
with no build step, and Athena reads it with the same LazySimpleSerDe it would use
for any text table. Staging is transient and read in full, so Parquet's column
pruning has nothing to prune.

The cost is a full scan of the staging text on merge. For a two-year deep tier
that is single-digit hundreds of megabytes, once, which is cents.

WHY NO HEADER ROW. The Glue table declares the columns. A header row would be
read as data unless `skip.header.line.count` is set on the table, which is one
more place the two definitions can silently disagree. The column ORDER is
therefore the contract, which is why tests/awsnative/backfill/test_staging.py
pins it: if the order drifted from the DDL, Athena would put every value in the
wrong column and most of the types would still fit.

DECIMALS ARE NEVER REFORMATTED. Values arrive from parsers.py as their exact
source text and are written unchanged.
"""

from __future__ import annotations

import gzip
from collections.abc import Iterable, Sequence

from awsnative.backfill.parsers import ArchiveAggTrade, ArchiveKline
from awsnative.backfill.tiers import ARCHIVE_VENUE

# The contract with awsnative/sql/ddl/. Order is meaning; do not reorder either
# side alone.
KLINE_COLUMNS = (
    "instrument_id",
    "open_time_us",
    "close_time_us",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
)

TRADE_COLUMNS = (
    "instrument_id",
    "venue",
    "venue_symbol",
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "event_ts_us",
    "side",
)


def klines_csv_gz(instrument_id: str, bars: Sequence[ArchiveKline]) -> bytes:
    """Deep-tier bars as gzipped CSV, in KLINE_COLUMNS order."""
    return _gzip(
        _join(
            (
                instrument_id,
                str(bar.open_time_us),
                str(bar.close_time_us),
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.quote_volume,
                str(bar.trade_count),
                bar.taker_buy_base_volume,
            )
            for bar in bars
        )
    )


def trades_csv_gz(
    instrument_id: str, venue_symbol: str, trades: Sequence[ArchiveAggTrade]
) -> bytes:
    """Hot-tier trades as gzipped CSV, in TRADE_COLUMNS order.

    `venue` is a literal: only Binance publishes an archive of this shape, which
    is also why archive-derived bars carry `venue_coverage = 1` (spec §6.4).

    `venue_symbol` is carried rather than derived because `silver_trades` declares
    it (spec §5.2) and the archive CSV does not contain it. Deriving it downstream
    would need the instrument map inside SQL, which is the one place it does not
    already exist.
    """
    return _gzip(
        _join(
            (
                instrument_id,
                ARCHIVE_VENUE,
                venue_symbol,
                str(trade.agg_trade_id),
                trade.price,
                trade.quantity,
                str(trade.first_trade_id),
                str(trade.last_trade_id),
                str(trade.event_ts_us),
                str(trade.side),
            )
            for trade in trades
        )
    )


def _join(rows: Iterable[tuple[str, ...]]) -> str:
    return "".join(f"{','.join(row)}\n" for row in rows)


def _gzip(text: str) -> bytes:
    """Deterministic gzip: mtime=0 so two identical inputs give identical bytes.

    Without it every write differs in the header, which turns a re-run into a new
    object and makes any content comparison impossible.
    """
    return gzip.compress(text.encode(), mtime=0)
