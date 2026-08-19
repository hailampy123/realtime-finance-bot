"""Binance USD-M perpetual context: funding, open interest and positioning. Pure.

WHAT THIS BUYS. "Price rose" means one thing when open interest rose with it and
the opposite when open interest fell: the first says new money entered, the
second says shorts covered and the move has spent itself. Gold cannot tell those
apart, and this is the data that can.

FOUR ENDPOINT FAMILIES, ONE ROW. `/fapi/v1/premiumIndex` answers for every symbol
in a single call and carries mark price, index price, funding rate and next
funding time. Open interest and the four ratios are per-symbol REST calls with no
native stream. One poll is 41 requests against a limit of 1000 per five minutes.

EACH FAMILY KEEPS ITS OWN TIMESTAMP (spec §7.1). The ratio endpoints answer on
their own 5-minute grid, which need not equal the poll instant. Collapsing them
to one timestamp would throw away the only evidence that would show the grids had
drifted apart, so Bronze carries all of them and `snapshot_ts_us` records the
grid point the poll itself belongs to.

COMPONENTS, NOT JUST RATIOS (spec §4.2). Three of the four endpoints return the
numerator and denominator alongside the ratio; the archive returns the ratio
alone. Carrying components is what makes a rollup over live rows valid, because
an average of ratios is not the ratio of the aggregates -- the same rule that
forbids a stored `vwap` in Gold.

MISSING IS EMPTY, NEVER ZERO. A ratio of zero says every account is short. An
absent endpoint says nothing at all, and the two must not look alike downstream.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

# JSON decoded from an HTTP response is genuinely dynamic. Declaring it object
# and casting at every use adds noise that hides the two conversions that do
# matter -- the millisecond boundary and the decimal-as-text rule -- so the
# payload types stay Any and those two are explicit functions instead.
JsonObject = Mapping[str, Any]

GRID_MINUTES = 5
_MICROS_PER_MILLI = 1_000
_GRID_US = GRID_MINUTES * 60 * 1_000_000

# endpoint family -> (ratio field, long-side field, short-side field). The taker
# endpoint names its two sides buy and sell rather than long and short; they are
# the same shape and are stored in the same two columns.
RATIO_FIELDS: dict[str, tuple[str, str, str]] = {
    "toptrader_accounts": ("longShortRatio", "longAccount", "shortAccount"),
    "toptrader_positions": ("longShortRatio", "longAccount", "shortAccount"),
    "global_accounts": ("longShortRatio", "longAccount", "shortAccount"),
    "taker_volume": ("buySellRatio", "buyVol", "sellVol"),
}

RATIO_FAMILIES = tuple(RATIO_FIELDS)


@dataclass(frozen=True, slots=True)
class RatioObservation:
    """One ratio endpoint's answer: the ratio, its two components, its own instant."""

    ratio: str
    long_component: str
    short_component: str
    source_ts_us: int

    @classmethod
    def absent(cls) -> RatioObservation:
        """The endpoint did not answer. Distinct from every component being zero."""
        return cls(ratio="", long_component="", short_component="", source_ts_us=0)

    @classmethod
    def from_payload(cls, family: str, payload: JsonObject) -> RatioObservation:
        ratio_field, long_field, short_field = RATIO_FIELDS[family]
        return cls(
            ratio=str(payload[ratio_field]),
            long_component=str(payload[long_field]),
            short_component=str(payload[short_field]),
            source_ts_us=int(payload["timestamp"]) * _MICROS_PER_MILLI,
        )


@dataclass(frozen=True, slots=True)
class PerpContext:
    """One instrument's perpetual context at one poll. The Bronze row."""

    instrument_id: str
    venue_symbol: str
    poll_ts_us: int
    snapshot_ts_us: int
    mark_price: str
    index_price: str
    estimated_settle_price: str
    last_funding_rate: str
    interest_rate: str
    next_funding_time_us: int
    premium_index_ts_us: int
    open_interest: str
    open_interest_ts_us: int
    toptrader_accounts: RatioObservation
    toptrader_positions: RatioObservation
    global_accounts: RatioObservation
    taker_volume: RatioObservation

    def to_json(self) -> dict[str, str]:
        """Flat, string-only. Bronze is text; a nested object would need a struct
        column and a cast the merge does not want."""
        row = {
            "instrument_id": self.instrument_id,
            "venue_symbol": self.venue_symbol,
            "venue": "binance",
            "poll_ts_us": str(self.poll_ts_us),
            "snapshot_ts_us": str(self.snapshot_ts_us),
            "mark_price": self.mark_price,
            "index_price": self.index_price,
            "estimated_settle_price": self.estimated_settle_price,
            "last_funding_rate": self.last_funding_rate,
            "interest_rate": self.interest_rate,
            "next_funding_time_us": str(self.next_funding_time_us),
            "premium_index_ts_us": str(self.premium_index_ts_us),
            "open_interest": self.open_interest,
            "open_interest_ts_us": str(self.open_interest_ts_us),
        }
        for family in RATIO_FAMILIES:
            observation: RatioObservation = getattr(self, family)
            row[f"{family}_ratio"] = observation.ratio
            row[f"{family}_long"] = observation.long_component
            row[f"{family}_short"] = observation.short_component
            row[f"{family}_ts_us"] = str(observation.source_ts_us)
        return row


def floor_to_grid_us(poll_ts_us: int) -> int:
    """The 5-minute grid point `poll_ts_us` belongs to.

    The ratio endpoints answer on this grid, so flooring the poll instant to it is
    what lets a poll and the readings it collected share one key. Bronze keeps the
    raw instants too, so the flooring is auditable rather than lossy (spec §7.2).
    """
    if poll_ts_us <= 0:
        raise ValueError(f"poll_ts_us must be a positive instant, got {poll_ts_us}")
    return poll_ts_us - (poll_ts_us % _GRID_US)


def index_by_symbol(payload: Iterable[JsonObject], wanted: set[str]) -> dict[str, JsonObject]:
    """Index a whole-market response by symbol, keeping only `wanted`.

    premiumIndex answers for 874 symbols in one 194 KB call. That single call is
    why funding needs no WebSocket, and this filter is why 866 instruments nobody
    trades do not reach Bronze on every poll.
    """
    return {str(row["symbol"]): row for row in payload if str(row["symbol"]) in wanted}


def build_rows(
    *,
    pairs: Sequence[tuple[str, str]],
    premium_index: Iterable[JsonObject],
    open_interest: Mapping[str, JsonObject],
    ratios: Mapping[str, Mapping[str, Sequence[JsonObject]]],
    poll_ts_us: int,
) -> list[PerpContext]:
    """One row per instrument that premiumIndex answered for.

    An instrument absent from premiumIndex is dropped: without a mark price there
    is no funding context worth a row. Every other family degrades to empty
    instead, because losing a funding rate to a ratio endpoint's timeout would be
    the wrong trade.
    """
    snapshot_ts_us = floor_to_grid_us(poll_ts_us)
    indexed = index_by_symbol(premium_index, {symbol for _, symbol in pairs})

    rows: list[PerpContext] = []
    for instrument_id, venue_symbol in pairs:
        premium = indexed.get(venue_symbol)
        if premium is None:
            continue
        interest = open_interest.get(venue_symbol, {})
        family_payloads = ratios.get(venue_symbol, {})
        rows.append(
            PerpContext(
                instrument_id=instrument_id,
                venue_symbol=venue_symbol,
                poll_ts_us=poll_ts_us,
                snapshot_ts_us=snapshot_ts_us,
                mark_price=str(premium.get("markPrice", "")),
                index_price=str(premium.get("indexPrice", "")),
                estimated_settle_price=str(premium.get("estimatedSettlePrice", "")),
                last_funding_rate=str(premium.get("lastFundingRate", "")),
                interest_rate=str(premium.get("interestRate", "")),
                next_funding_time_us=_millis_to_micros(premium.get("nextFundingTime")),
                premium_index_ts_us=_millis_to_micros(premium.get("time")),
                open_interest=str(interest.get("openInterest", "")),
                open_interest_ts_us=_millis_to_micros(interest.get("time")),
                **{f: _latest(f, family_payloads.get(f)) for f in RATIO_FAMILIES},
            )
        )
    return rows


def _latest(family: str, payload: Sequence[JsonObject] | None) -> RatioObservation:
    """The most recent row of a ratio endpoint's answer.

    The endpoints return oldest first. Taking the first row would record a reading
    five minutes stale on every poll -- invisible in the data, and wrong.
    """
    if not payload:
        return RatioObservation.absent()
    return RatioObservation.from_payload(family, payload[-1])


def _millis_to_micros(value: Any) -> int:
    """Binance answers in milliseconds; everything else here is microseconds.

    Converting at the boundary means nothing downstream has to remember which API
    a column came from. A missing or zero instant stays zero, which reads as
    "absent" rather than as 1970.
    """
    if value in (None, ""):
        return 0
    return int(value) * _MICROS_PER_MILLI
