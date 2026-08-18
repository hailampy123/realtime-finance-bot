"""Terraform and config/universe.yaml must name the same instruments.

Terraform cannot read the YAML, so the universe is written twice: once in
config/universe.yaml, which the collectors and the backfill read, and once as a
default in infra/envs/native/variables.tf, which the schedule's input uses. Two
copies of one fact drift, and the symptom would be an instrument that is ingested
but never enriched -- with no error anywhere, because nothing asks the two to
agree. This test is what asks.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from awsnative.backfill.seed import instrument_pairs

VARIABLES_TF = Path("infra/envs/native/variables.tf")
UNIVERSE = Path("config/universe.yaml")


def terraform_pairs() -> list[list[str]]:
    """The default of enrichment_instrument_pairs, as a list of pairs."""
    text = VARIABLES_TF.read_text()
    block = re.search(
        r'variable "enrichment_instrument_pairs".*?default\s*=\s*\[(.*?)\n  \]',
        text,
        re.DOTALL,
    )
    assert block, "enrichment_instrument_pairs has no parseable default"
    return [json.loads(row) for row in re.findall(r"\[[^\[\]]*\]", block.group(1))]


def test_the_two_universes_match_exactly() -> None:
    assert sorted(terraform_pairs()) == sorted([list(pair) for pair in instrument_pairs(UNIVERSE)])


def test_the_universe_is_not_empty() -> None:
    """Guards the test above: two empty lists are equal and prove nothing."""
    assert len(terraform_pairs()) == 8


def test_every_pair_is_canonical_id_then_binance_symbol() -> None:
    for instrument_id, venue_symbol in terraform_pairs():
        assert instrument_id.endswith("-USD")
        assert venue_symbol.endswith("USDT")
