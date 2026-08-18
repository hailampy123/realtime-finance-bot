from __future__ import annotations

from decimal import Decimal

import pytest

from awsnative.bars import Measures, bar_from_trades, rollup


def minute_bar(prices: list[str], sizes: list[str], sides: list[str]):
    return bar_from_trades(
        [Decimal(p) for p in prices],
        [Decimal(s) for s in sizes],
        sides,
    )


# Five minutes of deliberately uneven trading. The unevenness is the test:
# with equal volume per minute, averaging the per-minute VWAPs would happen to
# give the right answer and the whole design decision would look unnecessary.
MINUTES = [
    (["100", "101"], ["1", "1"], ["BUY", "SELL"]),
    (["102", "98"], ["50", "10"], ["BUY", "BUY"]),
    (["99"], ["0.5"], ["SELL"]),
    (["105", "103", "104"], ["2", "3", "100"], ["SELL", "SELL", "BUY"]),
    (["104"], ["0.25"], ["BUY"]),
]


@pytest.fixture
def one_minute_bars():
    return [minute_bar(*m) for m in MINUTES]


@pytest.fixture
def five_minute_bar():
    """The same trades aggregated in one pass, at 5-minute grain."""
    prices = [p for m in MINUTES for p in m[0]]
    sizes = [s for m in MINUTES for s in m[1]]
    sides = [s for m in MINUTES for s in m[2]]
    return minute_bar(prices, sizes, sides)


def test_vwap_is_grain_invariant(one_minute_bars, five_minute_bar) -> None:
    """The property the whole of spec 5.3 exists to buy.

    SUM(notional)/SUM(volume) over five 1-minute bars must equal the VWAP of
    the same trades aggregated directly. If Gold stored a vwap column instead
    of its two components, this would fail.
    """
    assert rollup(one_minute_bars).vwap == rollup([five_minute_bar]).vwap


def test_flow_imbalance_is_grain_invariant(one_minute_bars, five_minute_bar) -> None:
    assert rollup(one_minute_bars).flow_imbalance == rollup([five_minute_bar]).flow_imbalance


def test_volume_and_count_are_grain_invariant(one_minute_bars, five_minute_bar) -> None:
    coarse = rollup([five_minute_bar])
    fine = rollup(one_minute_bars)
    assert fine.volume == coarse.volume
    assert fine.trade_count == coarse.trade_count


def test_high_and_low_are_grain_invariant(one_minute_bars, five_minute_bar) -> None:
    """max-of-maxima is the true maximum, which is why these need no components."""
    coarse = rollup([five_minute_bar])
    fine = rollup(one_minute_bars)
    assert fine.high == coarse.high
    assert fine.low == coarse.low


def test_averaging_per_minute_vwaps_gives_the_wrong_answer(one_minute_bars) -> None:
    """The mistake the design avoids, demonstrated rather than asserted.

    This is what a `vwap` column in Gold would produce downstream. It is close
    enough to look right and wrong enough to matter, which is precisely why it
    has to be prevented structurally instead of by review.
    """
    naive = sum((b.notional / b.volume for b in one_minute_bars), Decimal(0)) / len(one_minute_bars)
    correct = rollup(one_minute_bars).vwap
    assert naive != correct
    assert abs(naive - correct) > Decimal("0.5")


def test_realized_vol_composes_additively(one_minute_bars) -> None:
    """SQRT(SUM(sq_log_return)) -- the component sums even though the value is
    tied to the sampling frequency it was measured at."""
    from math import sqrt

    expected = sqrt(sum(b.sq_log_return for b in one_minute_bars))
    assert rollup(one_minute_bars).realized_vol == pytest.approx(expected)


def test_realized_vol_is_deliberately_not_grain_invariant(one_minute_bars, five_minute_bar) -> None:
    """Not a bug, and this test exists so nobody "fixes" it into one.

    Realized variance is the sum of squared returns at a chosen sampling
    frequency. Rolling 1-minute bars up to 5 minutes yields the 1-minute-sampled
    figure over a 5-minute window, which is a different and more informative
    quantity than a 5-minute-sampled one. Unlike VWAP, there is no grain at
    which both are "the" answer.
    """
    assert rollup(one_minute_bars).realized_vol != pytest.approx(
        rollup([five_minute_bar]).realized_vol
    )


def test_a_rollup_refuses_to_report_open_or_close() -> None:
    """Spec 5.3's one documented non-additive exception, enforced by the type.

    Returning an arbitrary constituent bar's open would be worse than declining
    to answer: it is a plausible number that is wrong.
    """
    assert not hasattr(Measures, "open")
    assert not hasattr(Measures, "close")
    assert "open" not in Measures.__annotations__
    assert "close" not in Measures.__annotations__


def test_rollup_rejects_an_empty_window() -> None:
    """ "No bars" must not read as "zero volume" -- that is the vacuous green
    check spec 6.4 warns about."""
    with pytest.raises(ValueError, match="empty"):
        rollup([])


def test_bar_from_trades_rejects_an_empty_bar() -> None:
    with pytest.raises(ValueError, match="at least one trade"):
        bar_from_trades([], [], [])
