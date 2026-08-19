"""Binance archive CSV to typed rows. Pure; no network, no clock, no AWS.

Two archive layouts, neither with a header row, both documented only by example
in binance/binance-public-data. The column orders below were confirmed against
downloaded files rather than read off documentation.

THE TRAP THIS MODULE EXISTS FOR (spec §6.3). `isBuyerMaker = True` means the
BUYER was the maker, so the aggressor -- the side that crossed the spread -- was
the SELLER. Reading it the other way inverts every flow-imbalance feature, and
it fails QUIETLY: the rows stay well-formed and merely describe the opposite
market. `_side_from_buyer_maker` is the one place that mapping lives.

DECIMALS STAY AS THEIR SOURCE TEXT. `price` and `quantity` are never parsed to
float here. "0.00007000" through a float and back is a different string, and for
a price that difference is the bug. Staging is CSV read by Athena as text and the
merge casts to DECIMAL(38, 18), so the exact digits survive end to end. Same
discipline Bronze already applies to `price` and `size` (spec §5.1).

NO ARITHMETIC HAPPENS HERE. `sell_vol = volume - taker_buy_base_volume` and
`sq_log_return = ln(close/open)^2` are both derived in SQL, so the archive path
and the stream path compute them with one definition rather than two (spec §6.2:
one transform engine).
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from awsnative.backfill.epoch import UnitNormaliser
from ingest.core.models import Side

AGG_TRADE_COLUMNS = 8
KLINE_COLUMNS = 12

# Binance writes Python-style booleans in the archive, in both the millisecond
# and the microsecond era. Not "true"/"false", not "0"/"1".
_BOOLEANS = {"True": True, "False": False}


@dataclass(frozen=True, slots=True)
class ArchiveAggTrade:
    """One aggregated trade, normalised. Feeds Silver via archive_staging."""

    agg_trade_id: int
    price: str
    quantity: str
    first_trade_id: int
    last_trade_id: int
    event_ts_us: int
    side: Side


@dataclass(frozen=True, slots=True)
class ArchiveKline:
    """One 1-minute bar, normalised. Feeds Gold directly (spec §6.1).

    Deep-tier klines are bars, not trades, and must never be merged into
    `silver_trades` -- that would put bar rows in a trade table.
    """

    open_time_us: int
    close_time_us: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    quote_volume: str
    trade_count: int
    taker_buy_base_volume: str
    taker_buy_quote_volume: str


def parse_agg_trades(lines: Iterable[str]) -> Iterator[ArchiveAggTrade]:
    """Parse `aggTrades` CSV rows.

    Column order, confirmed against a real file:
        aggTradeId, price, quantity, firstTradeId, lastTradeId,
        timestamp, isBuyerMaker, isBestMatch

    `isBestMatch` is read and discarded: it is true for every row in every file
    inspected, so keeping it would add a column that carries no information.
    """
    normalise = UnitNormaliser()
    for line in lines:
        fields = _fields(line, AGG_TRADE_COLUMNS, "aggTrades")
        if fields is None:
            continue
        yield ArchiveAggTrade(
            agg_trade_id=int(fields[0]),
            price=fields[1],
            quantity=fields[2],
            first_trade_id=int(fields[3]),
            last_trade_id=int(fields[4]),
            event_ts_us=normalise(int(fields[5])),
            side=_side_from_buyer_maker(fields[6]),
        )


def parse_klines(lines: Iterable[str]) -> Iterator[ArchiveKline]:
    """Parse 1-minute `klines` CSV rows.

    Column order, confirmed against a real file:
        openTime, open, high, low, close, volume, closeTime, quoteAssetVolume,
        numberOfTrades, takerBuyBaseAssetVolume, takerBuyQuoteAssetVolume, ignore

    `openTime` and `closeTime` go through the SAME normaliser, so a file whose
    two time columns disagree about the unit raises rather than producing a bar
    whose window is 1000x too long.
    """
    normalise = UnitNormaliser()
    for line in lines:
        fields = _fields(line, KLINE_COLUMNS, "klines")
        if fields is None:
            continue
        yield ArchiveKline(
            open_time_us=normalise(int(fields[0])),
            open=fields[1],
            high=fields[2],
            low=fields[3],
            close=fields[4],
            volume=fields[5],
            close_time_us=normalise(int(fields[6])),
            quote_volume=fields[7],
            trade_count=int(fields[8]),
            taker_buy_base_volume=fields[9],
            taker_buy_quote_volume=fields[10],
        )


def _fields(line: str, expected: int, layout: str) -> list[str] | None:
    """Split one CSV line, or None if the line is blank.

    No `csv` module: these files have no quoting and no embedded commas, and
    `str.split` makes the column count explicit in the error.
    """
    stripped = line.strip()
    if not stripped:
        return None
    fields = stripped.split(",")
    if len(fields) != expected:
        raise ValueError(
            f"{layout} row has {len(fields)} fields, expected {expected} columns: {stripped!r}"
        )
    return fields


def _side_from_buyer_maker(text: str) -> Side:
    """THE TRAP, in one place.

    `isBuyerMaker = True` -> the buyer posted the resting order -> the taker sold
    -> `Side.SELL`. There is deliberately no default: an unrecognised value means
    Binance changed the format, and inventing a direction for it would produce
    exactly the quiet, well-formed, wrong data this whole module guards against.
    """
    try:
        buyer_was_maker = _BOOLEANS[text]
    except KeyError:
        raise ValueError(
            f"is_buyer_maker: expected 'True' or 'False', got {text!r}. "
            "Binance changed the archive's boolean format; update the parser "
            "rather than defaulting, which would invent an aggressor side."
        ) from None
    return Side.SELL if buyer_was_maker else Side.BUY
