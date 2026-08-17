"""Normalise Binance archive timestamps to microseconds.

THE TRAP THIS MODULE EXISTS FOR (spec §6.3). Binance changed the timestamp unit
in its public archive from milliseconds to microseconds partway through the
window this project backfills. Verified against the first `open_time` of real
`klines/BTCUSDT/1m/<date>.zip` files:

    2023-06-15   1686787200000       13 digits, milliseconds
    2024-06-15   1718409600000       13 digits, milliseconds
    2025-06-15   1749945600000000    16 digits, microseconds
    2026-08-15   1786752000286442    16 digits, microseconds

Assuming either unit puts part of the history off by 1000x, and it fails
QUIETLY: the rows parse, every type is right, and the bars simply land in the
wrong century. Nothing downstream rejects a well-formed row with a bad instant.

Detection is by magnitude, and that is safe rather than a guess because the two
plausible ranges sit three orders of magnitude apart with nothing in between:

    2015-01-01 .. 2040-01-01 as milliseconds    1.42e12 .. 2.21e12
    2015-01-01 .. 2040-01-01 as microseconds    1.42e15 .. 2.21e15

A value in neither range is not a timestamp this project should accept in either
unit. Seconds (1.4e9) and nanoseconds (1.4e18) both land outside, so a caller
who hands in the wrong unit gets an error instead of a silent 1000x error.

Detection is per FILE, not per value, which is why UnitNormaliser exists and why
it raises on disagreement. A row in the other unit does not mean "normalise this
one differently" -- it means the file is not what the parser thinks it is, and
continuing would mix two eras inside one partition.
"""

from __future__ import annotations

from enum import StrEnum

# 2015-01-01T00:00:00Z and 2040-01-01T00:00:00Z, in seconds. The lower bound sits
# below Binance's launch and the upper bound far past any file this will read, so
# widening either does not require re-reasoning about the gap between the ranges.
_EPOCH_FLOOR_S = 1_420_070_400
_EPOCH_CEIL_S = 2_208_988_800

MILLIS_RANGE = (_EPOCH_FLOOR_S * 1_000, _EPOCH_CEIL_S * 1_000)
MICROS_RANGE = (_EPOCH_FLOOR_S * 1_000_000, _EPOCH_CEIL_S * 1_000_000)

_MICROS_PER_MILLI = 1_000


class TimestampUnit(StrEnum):
    MILLIS = "ms"
    MICROS = "us"


class TimestampUnitError(ValueError):
    """A value is not a plausible instant, or a file disagrees with itself."""


def unit_of(value: int) -> TimestampUnit:
    """Which unit `value` is expressed in, or raise if it is neither."""
    if MILLIS_RANGE[0] <= value < MILLIS_RANGE[1]:
        return TimestampUnit.MILLIS
    if MICROS_RANGE[0] <= value < MICROS_RANGE[1]:
        return TimestampUnit.MICROS
    raise TimestampUnitError(
        f"{value} is not a plausible instant in milliseconds {MILLIS_RANGE} "
        f"or microseconds {MICROS_RANGE}"
    )


def to_micros(value: int, unit: TimestampUnit) -> int:
    """Scale `value` from `unit` to microseconds."""
    return value * _MICROS_PER_MILLI if unit is TimestampUnit.MILLIS else value


class UnitNormaliser:
    """Detects the unit from the first value, then holds every later value to it.

    One instance per file. Reusing one across files would let a millisecond file
    silently set the unit for a microsecond file that follows it.
    """

    __slots__ = ("_unit",)

    def __init__(self) -> None:
        self._unit: TimestampUnit | None = None

    @property
    def unit(self) -> TimestampUnit | None:
        """The unit detected so far, or None before the first value."""
        return self._unit

    def __call__(self, value: int) -> int:
        observed = unit_of(value)
        if self._unit is None:
            self._unit = observed
        elif observed is not self._unit:
            raise TimestampUnitError(
                f"{value} reads as {observed.value}, but this file disagrees: "
                f"earlier values read as {self._unit.value}"
            )
        return to_micros(value, observed)
