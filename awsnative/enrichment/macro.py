"""FRED macro series, stamped with the vintage that makes them honest. Pure.

WHAT THIS BUYS. A long call that only works when the dollar falls is a macro
trade wearing a crypto label. Without a macro series neither the agent nor the
backtest can tell which one it made.

THE PROBLEM THIS MODULE EXISTS TO SOLVE. Macro data is REVISED. A CPI observation
is stamped with the month it measures, published about six weeks later, and then
revised -- seasonally-adjusted CPI is recalculated every February for the
preceding five years. Verified against real ALFRED responses: August 2025 CPI
read 323.364 in the January 2026 vintage and 323.291 in the April 2026 vintage,
and 59 of 947 overlapping observations changed across that boundary.

So joining macro on `observation_date <= as_of` lets a backtest standing on
2025-09-01 read 323.291, a number that did not exist until February 2026. Joining
on `vintage_date <= as_of` reads 323.364, which is what the market read. That is
the entire point of the `knowledge_ts` invariant, and macro is the data class
that forces it.

ALWAYS ALFRED, ALWAYS AN EXPLICIT VINTAGE. Plain FRED returns current values, so
`vintage_date` would be a proxy for "whenever we happened to pull". ALFRED with
an explicit vintage makes it a real knowledge boundary for every series, and it
also truncates the observation set correctly: an observation published after the
vintage simply is not in the response.

NO API KEY. ALFRED's CSV export needs none -- verified against the live endpoint
for all six series. The parent design's §7.3 claim that there is "no API key
anywhere at all" therefore survives this slice intact, and no Secrets Manager
secret is added.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from io import StringIO

ALFRED_CSV_BASE = "https://alfred.stlouisfed.org/graph/alfredgraph.csv"


@dataclass(frozen=True, slots=True)
class MacroSeries:
    series_id: str
    description: str
    frequency: str
    revised: bool


# Six series. The 2s10s spread is deliberately absent: DGS10 - DGS2 is derivable,
# and the rule that forbids a stored vwap in Gold forbids a stored spread here.
#
# CPIAUCSL is the only revised series and is in the set for exactly that reason:
# it is what makes the vintage machinery load-bearing rather than decorative. The
# five market series are published once and never restated, so on their own they
# would exercise none of it. If CPIAUCSL is ever cut, cut the vintage handling
# with it and record that as a named cut.
SERIES: tuple[MacroSeries, ...] = (
    MacroSeries("DTWEXBGS", "Nominal broad U.S. dollar index", "daily", revised=False),
    MacroSeries("DGS2", "2-year Treasury constant maturity yield", "daily", revised=False),
    MacroSeries("DGS10", "10-year Treasury constant maturity yield", "daily", revised=False),
    MacroSeries("VIXCLS", "CBOE volatility index", "daily", revised=False),
    MacroSeries("SP500", "S&P 500 index", "daily", revised=False),
    MacroSeries(
        "CPIAUCSL", "CPI, all urban consumers, seasonally adjusted", "monthly", revised=True
    ),
)

SERIES_BY_ID = {series.series_id: series for series in SERIES}


@dataclass(frozen=True, slots=True)
class MacroObservation:
    """One value of one series, as it stood on one vintage date."""

    series_id: str
    observation_date: str
    vintage_date: str
    value: str

    def to_json(self) -> dict[str, str]:
        return {
            "series_id": self.series_id,
            "observation_date": self.observation_date,
            "vintage_date": self.vintage_date,
            "value": self.value,
        }


def alfred_csv_url(series_id: str, vintage_date: str) -> str:
    """The keyless CSV export for one series as of one vintage."""
    return f"{ALFRED_CSV_BASE}?id={series_id}&vintage_date={vintage_date}"


def parse_alfred_csv(
    text: str,
    *,
    series_id: str,
    vintage_date: str,
    since: str | None = None,
) -> list[MacroObservation]:
    """Parse an ALFRED CSV export into vintage-stamped observations.

    The value column is named `<SERIES>_<VINTAGE>`, for example
    `CPIAUCSL_20260115`. Checking that prefix is what catches a mis-built URL that
    returned a different series, which would otherwise load silently and put one
    series' numbers under another's name.

    An empty cell is SKIPPED, never zeroed. FRED writes an empty value for an
    observation it has none for -- October 2025 CPI is empty in both real
    vintages used as fixtures. Zero would be a reading; absent is the truth.
    """
    reader = csv.reader(StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("empty ALFRED response") from None
    if len(header) != 2:
        raise ValueError(f"expected a two-column CSV, got header {header!r}")

    value_column = header[1]
    if not value_column.startswith(series_id):
        raise ValueError(
            f"asked for {series_id} but the response names {value_column!r}; the URL is built wrong"
        )

    observations = []
    for row in reader:
        if len(row) != 2:
            continue
        observation_date, value = row[0].strip(), row[1].strip()
        if not value:
            continue
        if since is not None and observation_date < since:
            continue
        observations.append(
            MacroObservation(
                series_id=series_id,
                observation_date=observation_date,
                vintage_date=vintage_date,
                value=value,
            )
        )
    return observations
