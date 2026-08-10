from __future__ import annotations

import json
from pathlib import Path
from typing import Any

NOTEBOOK = Path("notebooks/04_stream_product_research.ipynb")
MSK_NOTEBOOK = Path("notebooks/03_msk_live_experiment.ipynb")


def load_notebook() -> dict[str, Any]:
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))


def load_msk_notebook() -> dict[str, Any]:
    return json.loads(MSK_NOTEBOOK.read_text(encoding="utf-8"))


def code_source(notebook: dict[str, Any]) -> str:
    return "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )


def markdown_source(notebook: dict[str, Any]) -> str:
    return "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "markdown"
    )


def test_notebook_has_switchable_bounded_read_only_contract():
    source = code_source(load_notebook())

    assert 'os.environ.get("FDAI_TARGET", "local")' in source
    assert 'RUN_MODE = "quick"' in source
    assert 'TOPIC = "md.trades.v1"' in source
    assert '"quick": {"limit": 20_000, "seconds": 60.0}' in source
    assert '"deep": {"limit": 200_000, "seconds": 600.0}' in source
    assert "devlab.resolve(RUNTIME_TARGET)" in source
    assert 'offset_reset="earliest"' in source

    for forbidden in (".produce(", ".commit(", "create_topics", "to_csv(", "to_parquet("):
        assert forbidden not in source


def test_notebooks_do_not_contain_manual_aws_login_or_profile_cells():
    source = code_source(load_notebook()) + code_source(load_msk_notebook())

    assert "aws sso login" not in source
    assert "AWS_PROFILE" not in source


def test_every_code_cell_is_plain_compilable_python():
    notebook = load_notebook()

    for index, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] == "code":
            compile("".join(cell["source"]), f"cell-{index}", "exec")


def test_notebook_explains_the_research_scope():
    markdown = markdown_source(load_notebook())

    for heading in (
        "Source and coverage",
        "Freshness and latency",
        "Uniqueness and integrity",
        "Market activity",
        "Cross-venue market structure",
        "Data-product evolution",
    ):
        assert heading in markdown
    assert "not executable arbitrage" in markdown


def test_notebook_defines_every_research_result_and_runtime_invariant():
    source = code_source(load_notebook())

    for result in (
        "coverage",
        "latency_summary",
        "quality_metrics",
        "activity",
        "volatility_ranking",
        "spread_summary",
        "leadership",
        "recommendations",
    ):
        assert result in source

    for invariant in (
        'assert (clean_df["size"] >= 0).all()',
        "assert not clean_df.duplicated(subset=NATURAL_KEY).any()",
        'assert (clean_df["notional"] >= 0).all()',
        'assert recommendations["metric"].str.len().gt(0).all()',
        'assert recommendations["measured_value"].notna().all()',
    ):
        assert invariant in source


def test_recommendations_have_an_evidence_contract():
    source = code_source(load_notebook())

    for field in (
        '"metric"',
        '"measured_value"',
        '"evidence_strength"',
        '"proposed_product"',
        '"layer"',
        '"validation_or_sla"',
        '"priority"',
    ):
        assert field in source
