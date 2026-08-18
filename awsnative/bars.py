"""The additive-measure contract for gold_bars_1m, as executable Python.

Spec section 5.3 says Gold stores numerators and denominators, never
precomputed ratios, so that a measure re-evaluated at any grain stays correct.
This module is that rule written down in a form a test can run, and
tests/awsnative/test_sql_contracts.py asserts the DDL declares every component
named here -- so adding a measure to one and forgetting the other fails
offline, before it becomes a column of quietly wrong numbers.

It is not the implementation. merge_gold_bars_1m.sql computes the bars and
Athena rolls them up; nothing in production imports this. What it buys is the
one property the SQL cannot prove about itself offline (section 9.1): that the
decomposition composes. tests/awsnative/test_bars.py rolls the same trades up
two ways and requires the answers to match, and query 6 of
verify_silver_gold.sql does the same thing against real data. Both, because
this file agreeing with itself would prove nothing.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from math import log, sqrt

# Roll up with SUM. These are the columns that make a measure re-derivable.
ADDITIVE_SUM = ("volume", "notional", "buy_vol", "sell_vol", "sq_log_return", "trade_count")

# Roll up with MAX / MIN. Additive in the sense that matters: the maximum of
# per-minute maxima is the true maximum.
ADDITIVE_EXTREMA = ("high", "low")

# NOT recoverable at a coarser grain, and the reason read paths expose OHLC at
# 1-minute grain only (spec 5.3). `open` is the first trade by event time and
# `close` the last; neither can be reconstructed from a set of bars, because a
# bar does not record which of its neighbours came first.
NON_ADDITIVE = ("open", "close")


@dataclass(frozen=True)
class Bar:
    """One row of gold_bars_1m, restricted to the measure columns."""

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    notional: Decimal
    buy_vol: Decimal
    sell_vol: Decimal
    sq_log_return: float
    trade_count: int


@dataclass(frozen=True)
class Measures:
    """What a rollup can honestly report.

    No `open` and no `close`, on purpose. Returning them would mean returning
    an arbitrary one of the constituent bars' values, which is worse than not
    answering: it is answering wrongly with a plausible number.
    """

    vwap: Decimal
    high: Decimal
    low: Decimal
    realized_vol: float
    flow_imbalance: Decimal
    volume: Decimal
    trade_count: int


def bar_from_trades(
    prices: Sequence[Decimal], sizes: Sequence[Decimal], sides: Sequence[str]
) -> Bar:
    """Build one bar from the trades inside it -- what merge_gold_bars_1m.sql does.

    Kept in step with the SQL by eye, not by machinery, which is the honest
    limit of offline testing here (spec 9.1). What the tests below it can still
    prove is that whatever this computes, it composes.
    """
    if not prices:
        raise ValueError("a bar needs at least one trade")

    volume = sum(sizes, Decimal(0))
    notional = sum((p * s for p, s in zip(prices, sizes, strict=True)), Decimal(0))
    buy_vol = sum((s for s, side in zip(sizes, sides, strict=True) if side == "BUY"), Decimal(0))
    sell_vol = sum((s for s, side in zip(sizes, sides, strict=True) if side == "SELL"), Decimal(0))
    open_, close = prices[0], prices[-1]
    return Bar(
        open=open_,
        high=max(prices),
        low=min(prices),
        close=close,
        volume=volume,
        notional=notional,
        buy_vol=buy_vol,
        sell_vol=sell_vol,
        sq_log_return=log(float(close) / float(open_)) ** 2,
        trade_count=len(prices),
    )


def rollup(bars: Sequence[Bar]) -> Measures:
    """Combine bars at any grain into the measures that survive combination."""
    if not bars:
        raise ValueError("cannot roll up an empty set of bars")

    volume = sum((b.volume for b in bars), Decimal(0))
    if volume <= 0:
        raise ValueError("cannot compute volume-weighted measures over zero volume")

    notional = sum((b.notional for b in bars), Decimal(0))
    buy_vol = sum((b.buy_vol for b in bars), Decimal(0))
    sell_vol = sum((b.sell_vol for b in bars), Decimal(0))
    return Measures(
        vwap=notional / volume,
        high=max(b.high for b in bars),
        low=min(b.low for b in bars),
        # The bar-internal estimator spec 5.3 specifies. Two things about it
        # are easy to get wrong and neither is a bug:
        #
        # It ignores the return from one bar's close to the next bar's open, so
        # it understates realized vol by the inter-bar component.
        #
        # And unlike vwap and flow_imbalance, its VALUE is grain-dependent by
        # definition -- realized variance is the sum of squared returns at a
        # chosen sampling frequency, so rolling five 1-minute bars up gives the
        # 1-minute-sampled realized vol over five minutes, which is a different
        # (and more informative) quantity than a 5-minute-sampled one. What
        # composes is the component, not the number. Do not "fix" a test that
        # finds those two disagreeing.
        realized_vol=sqrt(sum(b.sq_log_return for b in bars)),
        flow_imbalance=(buy_vol - sell_vol) / volume,
        volume=volume,
        trade_count=sum(b.trade_count for b in bars),
    )
