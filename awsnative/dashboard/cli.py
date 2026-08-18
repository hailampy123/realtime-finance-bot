"""Build the data-layer dashboard.

    python -m awsnative.dashboard --database fdai_native --workgroup fdai-native \
        --instrument BTC-USD --out dashboard.html

`build_page_from_rows` is pure and takes the query results, so the whole page
renders in a unit test with no AWS account. `main` is the only part that runs a
query or writes a file.

FRESHNESS BANDS LIVE HERE, NOT IN SQL. The query returns seconds; this module
decides what counts as stale. One definition, in the language that also renders
it, rather than a threshold baked into SQL and restated in the renderer.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from awsnative.dashboard.charts import (
    Box,
    Point,
    Series,
    grouped_bars,
    line_panel,
    rate_bars,
    stat_tile,
    table,
)
from awsnative.dashboard.page import Section, build_page

QUERY_DIR = Path(__file__).resolve().parent.parent / "sql" / "dashboard"

QUERIES = (
    "01_layer_counts.sql",
    "02_freshness.sql",
    "03_quarantine.sql",
    "04_perp_context.sql",
    "05_macro.sql",
)

# The micro-batch runs every five minutes, so anything under a quarter hour is
# simply the cadence. Past an hour a tick has been missed; past a day the stack is
# almost certainly down or torn down.
FRESH_SECONDS = 15 * 60
WARN_SECONDS = 60 * 60
SERIOUS_SECONDS = 24 * 60 * 60

Rows = list[dict[str, str]]


def _f(value: str | None) -> float | None:
    """Athena returns every cell as text, and an empty cell means NULL."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def freshness_tile(row: dict[str, str]) -> str:
    name = row.get("table_name", "?")
    lag = _f(row.get("lag_seconds"))
    count = int(_f(row.get("row_count")) or 0)

    if count == 0:
        # Never written is not the same condition as fallen behind, and the two
        # have different causes. Saying "no rows" beats reporting an enormous lag.
        return stat_tile(label=name, value="no rows", status="critical", note="never written")
    if lag is None:
        return stat_tile(label=name, value=f"{count:,}", status="warning", note="no timestamp")

    minutes = lag / 60
    if lag < FRESH_SECONDS:
        status, note = "good", "within the 5-minute cadence"
    elif lag < WARN_SECONDS:
        status, note = "warning", "a tick or more behind"
    elif lag < SERIOUS_SECONDS:
        status, note = "serious", "hours behind"
    else:
        status, note = "critical", "over a day behind"
    value = f"{minutes:,.0f} min" if minutes < 600 else f"{minutes / 60:,.1f} h"
    return stat_tile(label=name, value=value, status=status, note=f"{count:,} rows · {note}")


def _pivot(
    rows: Rows, *, key: str, series_field: str, value_field: str
) -> tuple[tuple[str, ...], tuple[Series, ...]]:
    categories = tuple(dict.fromkeys(r[key] for r in rows))
    names = tuple(dict.fromkeys(r[series_field] for r in rows))
    series = tuple(
        Series(
            name=name,
            slot=index,
            points=tuple(
                Point(x=0.0, y=_f(r[value_field]) or 0.0, label=r[key])
                for r in rows
                if r[series_field] == name
            ),
        )
        for index, name in enumerate(names)
    )
    return categories, series


def quarantine_panel(rows: Rows) -> str:
    """Quarantine as a RATE, not as two counts side by side.

    37 quarantined rows beside 398,112 accepted ones on a shared linear scale is a
    sub-pixel sliver: the reader sees nothing and concludes nothing was
    quarantined, which is the opposite of what spec §5.4 wants visible. The ratio
    is the measure that carries the meaning, and it fits on one axis.
    """
    accepted: dict[str, float] = {}
    quarantined: dict[str, float] = {}
    for row in rows:
        bucket = accepted if row["kind"] == "Accepted" else quarantined
        bucket[row["dt"]] = _f(row["row_count"]) or 0.0

    days = tuple(sorted(set(accepted) | set(quarantined)))
    points = tuple(
        Point(
            x=0.0,
            y=(
                100.0 * quarantined.get(day, 0.0) / total
                if (total := accepted.get(day, 0.0) + quarantined.get(day, 0.0))
                else 0.0
            ),
            label=day,
        )
        for day in days
    )
    return rate_bars(
        Series("Quarantine rate", points, slot=1),
        categories=days,
        title="Quarantine rate",
        subtitle="violations are never dropped, only diverted (spec §5.4)",
        suffix="%",
    )


def build_page_from_rows(
    *,
    results: dict[str, Rows],
    database: str,
    instrument_id: str,
    lookback_days: int,
    generated_at: str,
) -> str:
    """The whole page, from query results. Pure."""
    health_blocks = [freshness_tile(r) for r in results.get("02_freshness.sql", [])]

    counts = results.get("01_layer_counts.sql", [])
    categories, series = _pivot(counts, key="dt", series_field="layer", value_field="row_count")
    volume_blocks = []
    if counts:
        volume_blocks.append(
            grouped_bars(
                series,
                categories=categories,
                title="Rows landed per day, by layer",
                subtitle=f"last {lookback_days} days",
            )
        )
        volume_blocks.append(
            table(
                ("Day", "Layer", "Rows"),
                tuple((r["dt"], r["layer"], r["row_count"]) for r in counts),
                caption="Rows landed per day",
            )
        )

    quarantine = results.get("03_quarantine.sql", [])
    if quarantine:
        volume_blocks.append(quarantine_panel(quarantine))

    perp = results.get("04_perp_context.sql", [])
    showcase = []
    for index, (field, title, subtitle, zero) in enumerate(
        (
            ("mark_price", "Mark price", "what the tape already told you", False),
            ("funding_rate", "Funding rate", "who is paying to hold the position", True),
            ("open_interest", "Open interest", "is the move new money or shorts covering", False),
        )
    ):
        points = tuple(
            Point(x=_f(r["ts"]) or 0.0, y=_f(r[field]) or 0.0, label=r["label"])
            for r in perp
            if _f(r[field]) is not None
        )
        showcase.append(
            line_panel(
                Series(name=title, points=points, slot=index),
                title=f"{instrument_id} · {title}",
                subtitle=subtitle,
                box=Box(height=170),
                zero_baseline=zero,
            )
        )
    if perp:
        showcase.append(
            table(
                ("Time", "Mark price", "Funding rate", "Open interest"),
                tuple(
                    (r["label"], r["mark_price"], r["funding_rate"], r["open_interest"])
                    for r in perp[-40:]
                ),
                caption=f"{instrument_id} perpetual context (last 40 points)",
            )
        )

    macro = results.get("05_macro.sql", [])
    macro_tiles = tuple(
        stat_tile(
            label=r["series_id"],
            value=r["value"],
            status="good",
            note=f"obs {r['observation_date']} · vintage {r['vintage_date']}",
        )
        for r in macro
    )
    macro_table = (
        (
            table(
                ("Series", "Observation", "Vintage", "Value"),
                tuple(
                    (r["series_id"], r["observation_date"], r["vintage_date"], r["value"])
                    for r in macro
                ),
                caption="Macro, as known now",
            ),
        )
        if macro
        else ()
    )

    return build_page(
        title="Data layer — daily health and enrichment",
        generated_at=generated_at,
        database=database,
        lede=(
            "Top row: is the pipeline running and is anything being dropped. "
            "Below: what the derivatives and macro enrichment lets you see that "
            "price and size alone cannot."
        ),
        sections=(
            Section("Freshness", tuple(health_blocks), layout="tiles"),
            Section("Volume and quarantine", tuple(volume_blocks)),
            Section(f"Perpetual context · {instrument_id}", tuple(showcase)),
            Section("Macro regime", macro_tiles, layout="tiles"),
            Section("Macro detail", macro_table),
        ),
        footnotes=(
            "Macro values are filtered on vintage_date, never observation_date: a CPI "
            "print is stamped with the month it measures and published about six weeks "
            "later, then revised. Filtering on the observation date would show a "
            "backtest numbers that did not exist yet.",
            "The three perpetual panels share one x-axis and have separate y-axes on "
            "purpose. Price, a funding rate near zero and an open interest in the tens "
            "of thousands share no scale, and putting two of them on one plot with two "
            "y-scales would let the author choose the correlation you see.",
            "Every chart has a table view. Numbers here are whatever the tables held "
            "when the page was generated; the account is wiped weekly.",
        ),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", required=True)
    parser.add_argument("--workgroup", required=True)
    parser.add_argument("--instrument", default="BTC-USD")
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--out", type=Path, default=Path("dashboard.html"))
    args = parser.parse_args(argv)

    from awsnative.athena import AthenaError, AthenaRunner
    from awsnative.render import render

    runner = AthenaRunner(database=args.database, workgroup=args.workgroup)
    results: dict[str, Rows] = {}
    scanned = 0
    for name in QUERIES:
        sql = render(
            (QUERY_DIR / name).read_text(),
            database=args.database,
            lookback_days=args.lookback_days,
            instrument_id=args.instrument,
        )
        try:
            outcome = runner.execute(sql)
        except AthenaError as error:
            # One query failing must not cost the rest of the page. An absent
            # section is legible; a stack trace instead of a dashboard is not.
            print(f"  {name}: FAILED ({error})", file=sys.stderr)
            results[name] = []
            continue
        results[name] = runner.fetch_rows(outcome.query_execution_id, max_rows=1000)
        scanned += outcome.data_scanned_bytes
        print(f"  {name}: {len(results[name])} rows, {outcome.data_scanned_mb:.2f} MB")

    html = build_page_from_rows(
        results=results,
        database=args.database,
        instrument_id=args.instrument,
        lookback_days=args.lookback_days,
        generated_at=f"{datetime.now(tz=UTC):%Y-%m-%d %H:%M}",
    )
    args.out.write_text(html)
    print(f"\n{args.out} written ({len(html):,} bytes, {scanned / 1_048_576:.2f} MB scanned)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
