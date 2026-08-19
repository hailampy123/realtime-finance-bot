"""Which archive files a backfill window needs, and where they live. Pure.

`today` is always a parameter, never `date.today()`. A planner that reads the
clock cannot be tested for the one behaviour that matters here -- what it does at
a month boundary -- so the clock stays outside.

WHY MONTHLY WHERE POSSIBLE (amends spec §6.1's ~5,840-file estimate). The archive
publishes both daily and monthly files. A two-year deep tier is ~5,840 daily
files or ~192 monthly ones for eight instruments, and every file is one Lambda
invocation, one HTTPS round trip and two S3 requests. Reading monthly where the
window fully covers the month is the same data for ~4% of the calls.

Two rules keep that safe, and both are tested:

  1. A monthly file covers the WHOLE month, so it is used only when the window
     fully covers that month. Otherwise the plan would merge rows from outside
     the requested window and quietly change what the window means.
  2. Binance publishes a monthly file "at the first monday of the month", so a
     month that has just ended may have no monthly file yet. The planner waits
     MONTHLY_PUBLICATION_LAG_DAYS before it will ask for one. That is why there
     is no runtime 404 fallback: the plan never names a file that is not there.

Only Binance is here. Coinbase publishes no comparable archive, which is why
archive-derived bars carry `venue_coverage = 1` (spec §6.4).
"""

from __future__ import annotations

from calendar import monthrange
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum

ARCHIVE_VENUE = "binance"
ARCHIVE_BASE_URL = "https://data.binance.vision"

# Slack between a month ending and its monthly file appearing. Seven days covers
# "the first monday of the month" from any weekday the month ends on.
MONTHLY_PUBLICATION_LAG_DAYS = 7


class Tier(StrEnum):
    """A backfill tier. The mid tier (1s klines) is cut -- see spec §2."""

    DEEP = "DEEP"
    HOT = "HOT"


class Granularity(StrEnum):
    MONTHLY = "monthly"
    DAILY = "daily"


@dataclass(frozen=True, slots=True)
class _TierLayout:
    """Where a tier's files sit in the archive, and what they are called."""

    dataset: str
    interval_segment: str
    name_infix: str


# klines carry an interval in both the path and the filename; aggTrades do not.
_LAYOUTS = {
    Tier.DEEP: _TierLayout(dataset="klines", interval_segment="1m/", name_infix="1m"),
    Tier.HOT: _TierLayout(dataset="aggTrades", interval_segment="", name_infix="aggTrades"),
}


@dataclass(frozen=True, slots=True)
class ArchiveFile:
    """One archive object to fetch, and the manifest row that will track it."""

    tier: Tier
    instrument_id: str
    venue_symbol: str
    granularity: Granularity
    period: str
    dt: date
    """The first day this file covers. The manifest's date key, so a monthly file
    and the first daily file of the same month never collide."""

    @property
    def venue(self) -> str:
        return ARCHIVE_VENUE

    @property
    def filename(self) -> str:
        layout = _LAYOUTS[self.tier]
        return f"{self.venue_symbol}-{layout.name_infix}-{self.period}.zip"

    @property
    def key(self) -> str:
        layout = _LAYOUTS[self.tier]
        return (
            f"data/spot/{self.granularity.value}/{layout.dataset}/"
            f"{self.venue_symbol}/{layout.interval_segment}{self.filename}"
        )

    @property
    def url(self) -> str:
        return f"{ARCHIVE_BASE_URL}/{self.key}"

    @property
    def checksum_url(self) -> str:
        return f"{self.url}.CHECKSUM"


def files_for_window(
    pairs: Sequence[tuple[str, str]],
    tier: Tier,
    start: date,
    end: date,
    *,
    today: date,
) -> list[ArchiveFile]:
    """Every archive file needed to cover [start, end] inclusive, per instrument.

    `pairs` is (instrument_id, venue_symbol), as
    `InstrumentMap.symbols_for("binance")` plus `canonical()` produce.

    Ordered by (instrument, first day covered, granularity): chronological within
    an instrument, and identical across two identical calls. Sorting by key
    instead would be equally deterministic and would interleave the eras, because
    "daily" sorts before "monthly".

    The order is for whoever reads the manifest. Step Functions' Distributed Map
    fans the files out concurrently, so it carries no execution meaning.
    """
    if start > end:
        return []

    files: list[ArchiveFile] = []
    for instrument_id, venue_symbol in pairs:
        for first, last in _months_between(start, end):
            window_covers_month = first >= start and last <= end
            monthly_is_published = last + timedelta(days=MONTHLY_PUBLICATION_LAG_DAYS) <= today
            if window_covers_month and monthly_is_published:
                files.append(
                    ArchiveFile(
                        tier=tier,
                        instrument_id=instrument_id,
                        venue_symbol=venue_symbol,
                        granularity=Granularity.MONTHLY,
                        period=f"{first:%Y-%m}",
                        dt=first,
                    )
                )
                continue
            files.extend(
                ArchiveFile(
                    tier=tier,
                    instrument_id=instrument_id,
                    venue_symbol=venue_symbol,
                    granularity=Granularity.DAILY,
                    period=f"{day:%Y-%m-%d}",
                    dt=day,
                )
                for day in _days_between(max(first, start), min(last, end))
            )

    return sorted(files, key=lambda f: (f.instrument_id, f.dt, f.granularity.value))


def _months_between(start: date, end: date) -> Iterable[tuple[date, date]]:
    """(first day, last day) of every calendar month touching [start, end]."""
    cursor = start.replace(day=1)
    while cursor <= end:
        last = cursor.replace(day=monthrange(cursor.year, cursor.month)[1])
        yield cursor, last
        cursor = last + timedelta(days=1)


def _days_between(start: date, end: date) -> Iterable[date]:
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)
