import pytest

from ingest.core.instruments import InstrumentMap

YAML = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues:
      binance: BTCUSDT
      coinbase: BTC-USD
  - id: ETH-USD
    asset_class: crypto
    venues:
      binance: ETHUSDT
      coinbase: ETH-USD
"""


@pytest.fixture
def imap(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(YAML)
    return InstrumentMap.from_yaml(path)


def test_collapses_venue_symbols_to_one_canonical_id(imap):
    assert imap.canonical("binance", "BTCUSDT") == "BTC-USD"
    assert imap.canonical("coinbase", "BTC-USD") == "BTC-USD"


def test_symbols_for_returns_that_venues_tickers(imap):
    assert sorted(imap.symbols_for("binance")) == ["BTCUSDT", "ETHUSDT"]


def test_unknown_symbol_raises_rather_than_guessing(imap):
    with pytest.raises(KeyError, match="DOGEUSDT"):
        imap.canonical("binance", "DOGEUSDT")
