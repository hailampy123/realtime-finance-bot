"""The timestamp-unit trap, tested against instants taken from real archive files.

The four literals below are the first `open_time` of a real
`klines/BTCUSDT/1m/<date>.zip`. They are the evidence that the unit changes
partway through the backfill window, so they are asserted rather than described.
"""

from __future__ import annotations

import pytest

from awsnative.backfill.epoch import (
    TimestampUnit,
    TimestampUnitError,
    UnitNormaliser,
    to_micros,
    unit_of,
)

# Real first-row open_time values, one per era.
KLINE_2023 = 1_686_787_200_000
KLINE_2024 = 1_718_409_600_000
KLINE_2025 = 1_749_945_600_000_000
KLINE_2026 = 1_786_752_000_286_442


class TestUnitOf:
    @pytest.mark.parametrize("value", [KLINE_2023, KLINE_2024])
    def test_the_millisecond_era_reads_as_milliseconds(self, value: int) -> None:
        assert unit_of(value) is TimestampUnit.MILLIS

    @pytest.mark.parametrize("value", [KLINE_2025, KLINE_2026])
    def test_the_microsecond_era_reads_as_microseconds(self, value: int) -> None:
        assert unit_of(value) is TimestampUnit.MICROS

    @pytest.mark.parametrize(
        ("value", "what"),
        [
            (1_686_787_200, "seconds"),
            (1_686_787_200_000_000_000, "nanoseconds"),
            (0, "zero"),
            (-1_686_787_200_000, "negative"),
        ],
    )
    def test_a_value_in_neither_range_raises_rather_than_being_guessed(
        self, value: int, what: str
    ) -> None:
        # Seconds and nanoseconds are the two units a caller might plausibly
        # hand in by mistake. Both must fail loudly: silently reading seconds as
        # milliseconds would put every bar in 1970 and nothing downstream checks.
        with pytest.raises(TimestampUnitError):
            unit_of(value)


class TestToMicros:
    def test_milliseconds_scale_by_one_thousand(self) -> None:
        assert to_micros(KLINE_2023, TimestampUnit.MILLIS) == 1_686_787_200_000_000

    def test_microseconds_pass_through_unchanged(self) -> None:
        assert to_micros(KLINE_2025, TimestampUnit.MICROS) == KLINE_2025


class TestUnitNormaliser:
    def test_it_detects_from_the_first_value_and_holds_later_values_to_it(self) -> None:
        normalise = UnitNormaliser()
        assert normalise(KLINE_2023) == 1_686_787_200_000_000
        assert normalise.unit is TimestampUnit.MILLIS
        assert normalise(KLINE_2023 + 60_000) == 1_686_787_260_000_000

    def test_a_file_that_disagrees_with_itself_raises(self) -> None:
        # Detection is per file on purpose. A row in the other unit is not
        # something to normalise row by row -- it means the file is not what
        # this parser thinks it is, and continuing would mix two eras in one
        # partition.
        normalise = UnitNormaliser()
        normalise(KLINE_2023)
        with pytest.raises(TimestampUnitError, match="disagrees"):
            normalise(KLINE_2025)

    def test_unit_is_none_before_the_first_value(self) -> None:
        assert UnitNormaliser().unit is None
