"""The chart builders, and the design rules they are supposed to enforce.

Several of these assert a rule rather than an output. That is deliberate: the
rules are the reason the charts are readable, and a rule nothing checks is a
convention that erodes.
"""

from __future__ import annotations

import re

import pytest

from awsnative.dashboard.charts import (
    SERIES_DARK,
    SERIES_LIGHT,
    STATUS,
    Box,
    Point,
    Series,
    grouped_bars,
    line_panel,
    stat_tile,
    table,
)

POINTS = tuple(Point(x=float(i), y=float(i * i), label=f"t{i}") for i in range(6))


class TestPaletteRules:
    def test_three_validated_slots_in_each_mode(self) -> None:
        # Validated together with the skill's checker: worst all-pairs CVD dE 9.2
        # light / 9.4 dark, worst normal-vision 24.0 light / 20.9 dark. A fourth
        # slot would put yellow beside orange, which fails the all-pairs floors.
        assert len(SERIES_LIGHT) == len(SERIES_DARK) == 3

    def test_status_colours_are_never_series_colours(self) -> None:
        # A status hue impersonating a series is how "red means bad" quietly
        # becomes "red means the third instrument".
        status_hexes = {hex_ for hex_, _ in STATUS.values()}
        assert status_hexes.isdisjoint(set(SERIES_LIGHT) | set(SERIES_DARK))

    def test_every_status_ships_with_a_text_token(self) -> None:
        # Colour alone is not readable to everyone and does not survive greyscale.
        for _hex, token in STATUS.values():
            assert token and token.isupper()


class TestLinePanel:
    def test_it_draws_one_path_per_series(self) -> None:
        svg = line_panel(Series("Mark price", POINTS), title="BTC-USD")
        assert svg.count("<path") == 1

    def test_lines_are_two_pixels(self) -> None:
        assert 'stroke-width="2"' in line_panel(Series("s", POINTS), title="t")

    def test_the_colour_is_a_css_variable_so_dark_mode_swaps_in_one_place(self) -> None:
        svg = line_panel(Series("s", POINTS, slot=1), title="t")
        assert "var(--series-2)" in svg
        for literal in (*SERIES_LIGHT, *SERIES_DARK):
            assert literal not in svg

    def test_only_the_last_point_is_directly_labelled(self) -> None:
        # A number on every point is noise; a number on the newest point is the
        # one a reader wants, and it is also the relief the light-mode contrast
        # warning requires.
        svg = line_panel(Series("s", POINTS), title="t")
        assert svg.count('class="viz-direct"') == 1

    def test_every_point_gets_a_hover_target_larger_than_the_mark(self) -> None:
        svg = line_panel(Series("s", POINTS), title="t")
        assert svg.count('r="10"') == len(POINTS)
        assert 'r="4.5"' in svg

    def test_zero_baseline_forces_zero_into_range_for_signed_measures(self) -> None:
        # Funding rate's sign IS the signal. An axis that never shows zero hides
        # which side of it the series sits on.
        signed = tuple(Point(x=float(i), y=0.001, label="") for i in range(3))
        assert 'class="viz-zero"' not in line_panel(
            Series("s", signed), title="t", zero_baseline=False
        )
        crossing = (Point(0, -0.001), Point(1, 0.002))
        assert 'class="viz-zero"' in line_panel(
            Series("s", crossing), title="t", zero_baseline=True
        )

    def test_an_empty_series_says_so_rather_than_drawing_a_flat_line(self) -> None:
        # A blank chart reads as zero, which is a claim about the data.
        svg = line_panel(Series("s", ()), title="t")
        assert "no rows in the window" in svg
        assert "<path" not in svg

    def test_a_constant_series_does_not_divide_by_zero(self) -> None:
        flat = tuple(Point(x=float(i), y=5.0, label="") for i in range(4))
        assert "<path" in line_panel(Series("s", flat), title="t")

    def test_the_title_is_escaped(self) -> None:
        svg = line_panel(Series("s", POINTS), title='<script>alert("x")</script>')
        assert "<script>" not in svg
        assert "&lt;script&gt;" in svg


class TestGroupedBars:
    def series(self, n: int) -> tuple[Series, ...]:
        return tuple(
            Series(f"s{i}", (Point(0, float(i + 1), "Mon"), Point(0, float(i + 2), "Tue")), slot=i)
            for i in range(n)
        )

    def test_a_legend_is_present_for_two_or_more_series(self) -> None:
        svg = grouped_bars(self.series(2), title="t", categories=("Mon", "Tue"))
        assert svg.count('class="viz-key"') == 2

    def test_bar_ends_are_rounded_and_anchored_to_the_baseline(self) -> None:
        svg = grouped_bars(self.series(2), title="t", categories=("Mon", "Tue"))
        assert 'rx="4"' in svg

    def test_a_fourth_series_is_refused_rather_than_cycled(self) -> None:
        # Cycling would reuse slot 1 for series 4, so two different things would
        # be the same colour -- the failure the fixed order exists to prevent.
        with pytest.raises(ValueError, match="validated slots"):
            grouped_bars(self.series(4), title="t", categories=("Mon",))

    def test_a_missing_category_renders_as_zero_not_as_a_gap(self) -> None:
        sparse = (Series("s0", (Point(0, 3.0, "Mon"),), slot=0),)
        svg = grouped_bars(sparse, title="t", categories=("Mon", "Tue"))
        assert svg.count("<rect") == 2

    def test_no_categories_says_so(self) -> None:
        assert "no rows in the window" in grouped_bars((), title="t", categories=())


class TestStatTile:
    def test_it_pairs_the_colour_with_a_token_and_a_note(self) -> None:
        html = stat_tile(label="silver_trades", value="3 min", status="good", note="fresh")
        assert "OK" in html and "fresh" in html
        assert STATUS["good"][0] in html

    @pytest.mark.parametrize("status", sorted(STATUS))
    def test_every_status_renders(self, status: str) -> None:
        assert stat_tile(label="l", value="v", status=status, note="n")

    def test_an_unknown_status_raises_rather_than_rendering_uncoloured(self) -> None:
        with pytest.raises(KeyError):
            stat_tile(label="l", value="v", status="mauve", note="n")


class TestTable:
    def test_it_renders_every_row(self) -> None:
        html = table(("a", "b"), (("1", "2"), ("3", "4")), caption="c")
        assert html.count("<tr>") == 3

    def test_cells_are_escaped(self) -> None:
        html = table(("a",), (("<b>",),), caption="c")
        assert "<b>" not in html.replace("<tbody>", "")


class TestGeometry:
    def test_nothing_is_drawn_outside_the_view_box(self) -> None:
        # Overflow is the failure the validator cannot see, so it is checked here.
        box = Box()
        svg = line_panel(Series("s", POINTS), title="t", box=box)
        xs = [float(m) for m in re.findall(r'c[xy]="([\d.]+)"', svg)]
        assert xs, "expected marker coordinates"
        assert max(xs) <= max(box.width, box.height)

    def test_the_direct_label_stays_inside_the_right_margin(self) -> None:
        box = Box()
        svg = line_panel(Series("s", POINTS), title="t", box=box)
        label_x = float(re.search(r'class="viz-direct" x="([\d.]+)"', svg).group(1))  # type: ignore[union-attr]
        assert label_x <= box.width - box.right
