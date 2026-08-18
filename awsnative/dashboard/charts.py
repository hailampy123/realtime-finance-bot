"""Inline SVG chart builders. Pure: strings in, string out.

WHY HAND-ROLLED SVG. The page has to open from a file:// URL on a laptop with no
network, because that is what "show me the data layer works" means when the AWS
account is wiped weekly. A charting library would need a CDN or a bundle step;
an SVG string needs neither and is diffable in review.

DESIGN RULES THIS MODULE ENFORCES, and why each one is not taste:

  No dual axis, ever. Two measures on two y-scales let the author choose the
  correlation the reader sees. Price, funding and open interest are three
  panels sharing an x-axis instead -- small multiples -- so the comparison is
  the reader's to make.

  Categorical colour is assigned by slot in a fixed order, never cycled. The
  three slots used here were validated together: worst all-pairs CVD deltaE 9.2
  light and 9.4 dark, worst normal-vision 24.0 light and 20.9 dark.

  Light-mode aqua sits at 2.74:1 against the surface, below the 3:1 bar. The
  relief rule therefore applies and is honoured twice over: every series carries
  a direct label, and page.py emits a table view of the same numbers.

  Marks are thin, lines 2px, markers >= 8px, bar ends 4px-rounded and anchored
  to the baseline, adjacent bars separated by a 2px surface gap, grid and axes
  recessive.
"""

from __future__ import annotations

from dataclasses import dataclass
from html import escape

# Slots 1-3 of the validated categorical theme, light and dark. Assigned in this
# order and never cycled: a fourth series folds into "Other" or gets its own
# panel, because the fourth slot puts yellow beside orange and that pair fails
# the all-pairs floors.
SERIES_LIGHT = ("#2a78d6", "#eb6834", "#1baf7a")
SERIES_DARK = ("#3987e5", "#d95926", "#199e70")

# Fixed, never themed, never reused as a series colour. Always shipped with an
# icon and a label so state never rests on hue alone.
STATUS = {
    "good": ("#0ca30c", "OK"),
    "warning": ("#fab219", "WARN"),
    "serious": ("#ec835a", "STALE"),
    "critical": ("#d03b3b", "FAIL"),
}


@dataclass(frozen=True, slots=True)
class Point:
    x: float
    y: float
    label: str = ""


@dataclass(frozen=True, slots=True)
class Series:
    name: str
    points: tuple[Point, ...]
    slot: int = 0

    @property
    def colour_var(self) -> str:
        """A CSS custom property, so light and dark swap in one place."""
        return f"var(--series-{self.slot + 1})"


@dataclass(frozen=True, slots=True)
class Box:
    width: int = 720
    height: int = 200
    left: int = 64
    right: int = 16
    top: int = 16
    bottom: int = 32

    @property
    def plot_width(self) -> int:
        return self.width - self.left - self.right

    @property
    def plot_height(self) -> int:
        return self.height - self.top - self.bottom


def _scale(value: float, lo: float, hi: float, out_lo: float, out_hi: float) -> float:
    if hi == lo:
        return (out_lo + out_hi) / 2
    return out_lo + (value - lo) * (out_hi - out_lo) / (hi - lo)


def _nice(value: float, decimals: int | None = None) -> str:
    """A number a person reads, not a float repr.

    `decimals` is None for "pick a sensible default per magnitude": one decimal
    for thousands and up, two around unity, six below one -- because a funding
    rate of 0.00003686 is destroyed by anything shorter.

    An explicit int overrides all of that. axis_labels raises it until a set of
    ticks is mutually distinct, and a percentage passes 3 because six decimals on
    a rate is precision nobody asked for.
    """
    magnitude = abs(value)
    if magnitude >= 1_000_000_000:
        return f"{value / 1_000_000_000:.{1 if decimals is None else max(decimals, 1)}f}B"
    if magnitude >= 1_000_000:
        return f"{value / 1_000_000:.{1 if decimals is None else max(decimals, 1)}f}M"
    if magnitude >= 1_000:
        return f"{value / 1_000:.{1 if decimals is None else max(decimals, 1)}f}k"
    if magnitude == 0:
        return "0"
    if magnitude >= 1:
        places = 2 if decimals is None else max(decimals, 2)
        return f"{value:,.{places}f}".rstrip("0").rstrip(".")
    places = 6 if decimals is None else decimals
    return f"{value:.{places}f}".rstrip("0").rstrip(".")


def line_panel(
    series: Series,
    *,
    title: str,
    subtitle: str = "",
    box: Box | None = None,
    zero_baseline: bool = False,
) -> str:
    """One measure over time. A panel, not a chart: stack these to compare.

    `zero_baseline` forces zero into the y-range, which matters for funding rate:
    the sign is the signal, and a y-axis that never shows zero hides which side of
    it the series is on.
    """
    box = box or Box()
    if not series.points:
        return _empty_panel(title, "no rows in the window", box)

    xs = [p.x for p in series.points]
    ys = [p.y for p in series.points]
    y_lo, y_hi = min(ys), max(ys)
    if zero_baseline:
        y_lo, y_hi = min(y_lo, 0.0), max(y_hi, 0.0)
    if y_lo == y_hi:
        y_lo, y_hi = y_lo - 1, y_hi + 1

    def px(x: float) -> float:
        return _scale(x, min(xs), max(xs), box.left, box.width - box.right)

    def py(y: float) -> float:
        return _scale(y, y_lo, y_hi, box.height - box.bottom, box.top)

    grid = _grid(box, y_lo, y_hi, py)
    path = " ".join(
        f"{'M' if i == 0 else 'L'}{px(p.x):.1f},{py(p.y):.1f}" for i, p in enumerate(series.points)
    )
    last = series.points[-1]
    zero_line = ""
    if zero_baseline and y_lo < 0 < y_hi:
        zero_line = (
            f'<line class="viz-zero" x1="{box.left}" y1="{py(0):.1f}" '
            f'x2="{box.width - box.right}" y2="{py(0):.1f}"/>'
        )

    # A direct label on the last point, never a number on every point. This is
    # also the relief the contrast WARN on light-mode aqua requires.
    label_x = min(px(last.x) + 10, box.width - box.right - 4)
    marker = (
        f'<circle class="viz-end" cx="{px(last.x):.1f}" cy="{py(last.y):.1f}" r="4.5" '
        f'fill="{series.colour_var}"/>'
    )
    direct = (
        f'<text class="viz-direct" x="{label_x:.1f}" y="{py(last.y) - 9:.1f}" '
        f'text-anchor="end">{escape(_nice(last.y))}</text>'
    )

    hover = "".join(
        f'<g class="viz-hit"><circle cx="{px(p.x):.1f}" cy="{py(p.y):.1f}" r="10" '
        f'fill="transparent"/><title>{escape(p.label)}: {escape(_nice(p.y))}</title></g>'
        for p in series.points
    )

    return f"""<figure class="viz-panel">
  <figcaption><span class="viz-title">{escape(title)}</span>
    <span class="viz-sub">{escape(subtitle)}</span></figcaption>
  <svg viewBox="0 0 {box.width} {box.height}" role="img"
       aria-label="{escape(title)}. {escape(subtitle)}">
    {grid}{zero_line}
    <path d="{path}" fill="none" stroke="{series.colour_var}" stroke-width="2"
          stroke-linejoin="round" stroke-linecap="round"/>
    {marker}{direct}{hover}
  </svg>
</figure>"""


def grouped_bars(
    series: tuple[Series, ...],
    *,
    title: str,
    subtitle: str = "",
    categories: tuple[str, ...],
    box: Box | None = None,
) -> str:
    """Magnitude across a few categories, a handful of series.

    Capped at three series on purpose: past slot three the validated categorical
    order puts yellow beside orange, which fails the all-pairs separation floors.
    A fourth series folds into "Other" or becomes its own panel.
    """
    if len(series) > len(SERIES_LIGHT):
        raise ValueError(
            f"{len(series)} series exceeds the {len(SERIES_LIGHT)} validated slots; "
            "fold the rest into 'Other' or use small multiples"
        )
    box = box or Box(height=240)
    if not categories or not series:
        return _empty_panel(title, "no rows in the window", box)

    highest = max((p.y for s in series for p in s.points), default=0.0) or 1.0
    group_width = box.plot_width / len(categories)
    # 2px of surface between adjacent bars, which is what keeps two fills from
    # reading as one wide fill.
    bar_width = max((group_width - 12) / len(series) - 2, 3.0)

    bars = []
    for group, category in enumerate(categories):
        for index, one in enumerate(series):
            value = next((p.y for p in one.points if p.label == category), 0.0)
            height = _scale(value, 0, highest, 0, box.plot_height)
            x = box.left + group * group_width + 6 + index * (bar_width + 2)
            y = box.height - box.bottom - height
            bars.append(
                f'<g class="viz-hit"><rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" '
                f'height="{max(height, 0.5):.1f}" rx="4" fill="{one.colour_var}"/>'
                f"<title>{escape(one.name)} · {escape(category)}: "
                f"{escape(_nice(value))}</title></g>"
            )

    ticks = "".join(
        f'<text class="viz-tick" x="{box.left + i * group_width + group_width / 2:.1f}" '
        f'y="{box.height - 10}" text-anchor="middle">{escape(c)}</text>'
        for i, c in enumerate(categories)
    )
    legend = "".join(
        f'<span class="viz-key"><i style="background:{s.colour_var}"></i>{escape(s.name)}</span>'
        for s in series
    )

    return f"""<figure class="viz-panel">
  <figcaption><span class="viz-title">{escape(title)}</span>
    <span class="viz-sub">{escape(subtitle)}</span></figcaption>
  <div class="viz-legend">{legend}</div>
  <svg viewBox="0 0 {box.width} {box.height}" role="img"
       aria-label="{escape(title)}. {escape(subtitle)}">
    {_grid(box, 0, highest, lambda v: _scale(v, 0, highest, box.height - box.bottom, box.top))}
    {"".join(bars)}{ticks}
  </svg>
</figure>"""


def stat_tile(*, label: str, value: str, status: str, note: str) -> str:
    """A headline number with a state. Not a chart, and that is the point.

    One number answered by one number does not need a plot. The status colour is
    always paired with a text token, because a colour alone is not readable to
    everyone and does not survive a greyscale print.
    """
    colour, token = STATUS[status]
    return f"""<div class="viz-tile" data-status="{escape(status)}">
  <div class="viz-tile-label">{escape(label)}</div>
  <div class="viz-tile-value">{escape(value)}</div>
  <div class="viz-tile-state"><span class="viz-dot" style="background:{colour}"></span>
    <strong>{escape(token)}</strong> {escape(note)}</div>
</div>"""


def table(headers: tuple[str, ...], rows: tuple[tuple[str, ...], ...], *, caption: str) -> str:
    """The table view every chart on this page is backed by.

    Required rather than optional: light-mode aqua fails the 3:1 contrast bar, and
    the relief for that is visible labels or a table. It also makes the page
    usable with a screen reader and copyable into a spreadsheet.
    """
    head = "".join(f'<th scope="col">{escape(h)}</th>' for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in row) + "</tr>" for row in rows
    )
    return f"""<details class="viz-table">
  <summary>{escape(caption)} — table view</summary>
  <table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>
</details>"""


def axis_labels(lo: float, hi: float, count: int = 5) -> list[str]:
    """Tick labels that are distinct from each other.

    Formatting each tick from its own magnitude produces "64.3k, 64.2k, 64.2k"
    when the series spans a narrow band high above zero -- two rules with the same
    label, which reads as a rendering bug and hides the scale. The precision has
    to come from the STEP between ticks, so this raises decimals until every label
    differs, then stops.
    """
    values = [lo + (hi - lo) * i / (count - 1) for i in range(count)]
    for decimals in range(0, 9):
        labels = [_nice(v, decimals) for v in values]
        if len(set(labels)) == len(labels):
            return labels
    return [f"{v:g}" for v in values]


def _grid(box: Box, lo: float, hi: float, py: object) -> str:
    """Four recessive horizontal rules with labels. Chrome, not data."""
    lines = []
    labels = axis_labels(lo, hi)
    for step in range(5):
        value = lo + (hi - lo) * step / 4
        y = py(value)  # type: ignore[operator]
        lines.append(
            f'<line class="viz-grid" x1="{box.left}" y1="{y:.1f}" '
            f'x2="{box.width - box.right}" y2="{y:.1f}"/>'
            f'<text class="viz-tick" x="{box.left - 8}" y="{y + 4:.1f}" '
            f'text-anchor="end">{escape(labels[step])}</text>'
        )
    return "".join(lines)


def _empty_panel(title: str, reason: str, box: Box) -> str:
    """An empty window says so. A blank chart reads as zero, which is a claim."""
    return f"""<figure class="viz-panel viz-empty">
  <figcaption><span class="viz-title">{escape(title)}</span></figcaption>
  <div class="viz-nodata" style="height:{box.height // 2}px">{escape(reason)}</div>
</figure>"""


def rate_bars(
    series: Series,
    *,
    title: str,
    subtitle: str = "",
    categories: tuple[str, ...],
    suffix: str = "",
    decimals: int = 3,
    box: Box | None = None,
) -> str:
    """One measure across a few categories, every bar directly labelled.

    Use this instead of putting a small series beside a large one. Quarantined
    rows against accepted rows is 37 against 398,112: on a shared linear scale the
    small series is a sub-pixel sliver, which reads as zero and is the thing you
    most wanted to see. The ratio is the measure; the two counts are not.

    One series, so no legend -- the title names it.
    """
    box = box or Box(height=200)
    if not categories or not series.points:
        return _empty_panel(title, "no rows in the window", box)

    values = {p.label: p.y for p in series.points}
    highest = max(values.values()) or 1.0
    slot_width = box.plot_width / len(categories)
    bar_width = min(slot_width - 16, 56.0)

    bars = []
    for index, category in enumerate(categories):
        value = values.get(category, 0.0)
        height = _scale(value, 0, highest, 0, box.plot_height - 14)
        x = box.left + index * slot_width + (slot_width - bar_width) / 2
        y = box.height - box.bottom - height
        bars.append(
            f'<g class="viz-hit"><rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" '
            f'height="{max(height, 1.0):.1f}" rx="4" fill="{series.colour_var}"/>'
            f"<title>{escape(category)}: {escape(_nice(value))}{escape(suffix)}</title></g>"
            f'<text class="viz-direct" x="{x + bar_width / 2:.1f}" y="{y - 6:.1f}" '
            f'text-anchor="middle">{escape(_nice(value, decimals))}{escape(suffix)}</text>'
        )

    ticks = "".join(
        f'<text class="viz-tick" x="{box.left + i * slot_width + slot_width / 2:.1f}" '
        f'y="{box.height - 10}" text-anchor="middle">{escape(c)}</text>'
        for i, c in enumerate(categories)
    )
    return f"""<figure class="viz-panel">
  <figcaption><span class="viz-title">{escape(title)}</span>
    <span class="viz-sub">{escape(subtitle)}</span></figcaption>
  <svg viewBox="0 0 {box.width} {box.height}" role="img"
       aria-label="{escape(title)}. {escape(subtitle)}">
    {"".join(bars)}{ticks}
  </svg>
</figure>"""
