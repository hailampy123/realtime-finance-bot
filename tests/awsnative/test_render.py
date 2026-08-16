from __future__ import annotations

from pathlib import Path

import pytest

from awsnative import render

ALL_SQL = sorted(render.SQL_DIR.rglob("*.sql"))


def test_there_are_sql_files_to_check() -> None:
    """Guards every rglob-driven test below: an empty glob passes them all."""
    assert len(ALL_SQL) >= 8


@pytest.mark.parametrize("path", ALL_SQL, ids=lambda p: p.name)
def test_only_known_placeholders(path: Path) -> None:
    """A file may not invent a parameter no caller knows to supply.

    Terraform renders the same templates. If a .sql file grows a ${new_thing},
    Python's substitute() raises here and Terraform's templatefile() raises at
    plan time -- but only if someone remembers to update both. This test is
    what makes forgetting loud.
    """
    unknown = render.placeholders_in(path.read_text()) - render.KNOWN_PLACEHOLDERS
    assert not unknown, f"{path.name} uses unknown placeholder(s): {sorted(unknown)}"


@pytest.mark.parametrize("path", ALL_SQL, ids=lambda p: p.name)
def test_no_terraform_directive_syntax(path: Path) -> None:
    """`%{` is a Terraform templatefile directive and would break rendering.

    Nothing in Trino SQL needs it, but date_format patterns are full of `%`
    and `%{` is one keystroke away. Python would render such a file happily and
    Terraform would fail on the same file at apply time, which is the worst
    place to find out.
    """
    assert "%{" not in path.read_text(), f"{path.name} contains a Terraform directive marker"


@pytest.mark.parametrize("path", ALL_SQL, ids=lambda p: p.name)
def test_no_placeholder_inside_a_line_comment(path: Path) -> None:
    """A ${...} in a -- comment splices a multi-line fragment into one line.

    Everything after the splice point ends up commented out, and the statement
    still parses -- as a shorter, wrong statement. Found the hard way.
    """
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        stripped = line.lstrip()
        if stripped.startswith("--"):
            assert "${" not in stripped, f"{path.name}:{number} has a placeholder in a comment"


def test_valid_and_invalid_predicates_are_exact_complements() -> None:
    """Silver and quarantine must partition the input, with no row in neither.

    Not "look equivalent" -- literally X and NOT X over the same expression
    text, so no future edit can make them merely mostly complementary. This is
    the offline half of the no-silent-drop guarantee; query 3 of
    verify_silver_gold.sql is the online half.
    """
    statements = render.merge_statements("fdai_native")
    expr = render.valid_expr()

    silver_wrap = f"AND COALESCE({expr}, false)"
    quarantine_wrap = f"AND NOT COALESCE({expr}, false)"

    assert silver_wrap in statements[render.MERGE_SILVER]
    assert quarantine_wrap in statements[render.MERGE_QUARANTINE]
    # The negation is the same text with NOT in front, not a re-typed predicate.
    assert quarantine_wrap == silver_wrap.replace("AND COALESCE", "AND NOT COALESCE", 1)


def test_coalesce_wraps_the_predicate_on_both_sides() -> None:
    """Without COALESCE a NULL-bearing row is rejected by p AND by NOT p.

    Firehose writes NULL for any JSON key the Glue schema does not name, so
    this is a live failure mode, not a theoretical one -- and its symptom is a
    trade that exists in Bronze and in neither Silver table.
    """
    statements = render.merge_statements("fdai_native")
    for name in (render.MERGE_SILVER, render.MERGE_QUARANTINE):
        assert "COALESCE(" in statements[name], f"{name} does not NULL-guard the predicate"


def test_silver_merge_has_no_update_branch() -> None:
    """A trade is an immutable fact (spec 5.2), so insert-if-absent only.

    An UPDATE branch here would silently make the last writer win, which is
    exactly the SCD Type 1 behaviour the spec argues against needing.
    """
    silver = render.merge_statements("fdai_native")[render.MERGE_SILVER]
    assert "WHEN NOT MATCHED THEN" in silver
    assert "WHEN MATCHED" not in silver


def test_quarantine_merge_is_a_merge_not_an_insert() -> None:
    """The window overlaps between runs; an INSERT would re-add bad rows 288x/day."""
    from awsnative.athena import split_statements

    quarantine = render.merge_statements("fdai_native")[render.MERGE_QUARANTINE]
    # Strip the rationale header, which is most of the file.
    (executable,) = split_statements(quarantine)
    assert executable.startswith("MERGE INTO")
    assert "WHEN NOT MATCHED THEN" in executable


def test_every_rendered_statement_is_a_single_statement() -> None:
    """Step Functions passes one QueryString per Athena state; a file that
    somehow rendered into two would silently run only the first."""
    from awsnative.athena import split_statements

    for name, sql in render.merge_statements("fdai_native").items():
        assert len(split_statements(sql)) == 1, f"{name} rendered to more than one statement"
    for name, sql in render.ddl_statements("fdai_native", "s3://b/"):
        assert len(split_statements(sql)) == 1, f"{name} rendered to more than one statement"


def test_silver_merge_dedupes_within_the_source_batch() -> None:
    """MERGE's NOT MATCHED branch does nothing about duplicates inside one batch.

    Two source rows sharing (venue, trade_id) would both insert, putting a
    duplicate key in Silver and breaking the invariant query 2 of the
    verification SQL exists to police.
    """
    silver = render.merge_statements("fdai_native")[render.MERGE_SILVER]
    assert "row_number() OVER" in silver
    assert "PARTITION BY b.venue, b.trade_id" in silver


def test_merges_render_with_no_placeholder_left() -> None:
    for name, sql in render.merge_statements("fdai_native", lookback_days=2).items():
        assert "${" not in sql, f"{name} still has an unrendered placeholder"
        assert "fdai_native." in sql
        assert "interval '2' day" in sql or name == render.MERGE_GOLD


def test_gold_merge_embeds_the_dirty_cte() -> None:
    """The dirty set is composed in, not duplicated, so N4 can swap the source."""
    gold = render.merge_statements("fdai_native")[render.MERGE_GOLD]
    assert "SELECT DISTINCT" in gold
    assert "bronze_trades_stream" in gold
    assert "WITH dirty AS" in gold


def test_ddl_renders_a_warehouse_location_per_table() -> None:
    statements = dict(render.ddl_statements("fdai_native", "s3://some-bucket"))
    assert len(statements) == 3
    for name, sql in statements.items():
        assert "${" not in sql, f"{name} still has an unrendered placeholder"
        assert "'table_type'        = 'ICEBERG'" in sql
    assert "s3://some-bucket/silver_trades/" in statements["010_silver_trades.sql"]
    assert "s3://some-bucket/gold_bars_1m/" in statements["030_gold_bars_1m.sql"]


def test_ddl_tolerates_a_warehouse_with_or_without_a_trailing_slash() -> None:
    with_slash = dict(render.ddl_statements("fdai_native", "s3://b/"))
    without = dict(render.ddl_statements("fdai_native", "s3://b"))
    assert with_slash == without


def test_render_raises_on_a_missing_parameter() -> None:
    """safe_substitute would leave ${database} in the query and let Athena find it."""
    with pytest.raises(KeyError):
        render.render("SELECT * FROM ${database}.t")
