from ingest.connectors.base import Connector
from ingest.connectors.binance import BinanceConnector
from ingest.connectors.coinbase import CoinbaseConnector
from ingest.core.instruments import InstrumentMap


def test_both_connectors_satisfy_the_protocol(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(
        "instruments:\n"
        "  - id: BTC-USD\n"
        "    asset_class: crypto\n"
        "    venues: {binance: BTCUSDT, coinbase: BTC-USD}\n"
    )
    imap = InstrumentMap.from_yaml(path)
    assert isinstance(BinanceConnector(imap), Connector)
    assert isinstance(CoinbaseConnector(imap), Connector)
