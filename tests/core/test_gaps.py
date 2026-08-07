from ingest.core.gaps import Gap, SequenceTracker


def test_first_observation_is_never_a_gap():
    tracker = SequenceTracker()
    assert tracker.observe("binance", "BTCUSDT", 100) is None


def test_contiguous_sequences_report_no_gap():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    assert tracker.observe("binance", "BTCUSDT", 101) is None


def test_jump_reports_a_gap_with_the_missing_range():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    gap = tracker.observe("binance", "BTCUSDT", 105)
    assert gap == Gap(venue="binance", venue_symbol="BTCUSDT", last_seen=100, next_seen=105)
    assert gap.missing_count == 4


def test_symbols_are_tracked_independently():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    tracker.observe("binance", "ETHUSDT", 900)
    assert tracker.observe("binance", "BTCUSDT", 101) is None
    assert tracker.observe("binance", "ETHUSDT", 901) is None


def test_venues_are_tracked_independently():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTC-USD", 100)
    assert tracker.observe("coinbase", "BTC-USD", 5) is None


def test_out_of_order_or_replayed_sequences_are_not_gaps():
    """REST repair republishes older ids; that must not look like a new gap."""
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    assert tracker.observe("binance", "BTCUSDT", 98) is None
    assert tracker.observe("binance", "BTCUSDT", 100) is None
    assert tracker.observe("binance", "BTCUSDT", 101) is None


def test_none_sequence_is_ignored():
    """Kraken publishes no stable id; absence of a sequence is not a gap."""
    tracker = SequenceTracker()
    assert tracker.observe("kraken", "XBT/USD", None) is None
    assert tracker.observe("kraken", "XBT/USD", None) is None


def test_reset_forgets_the_watermark():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    tracker.reset("binance", "BTCUSDT")
    assert tracker.observe("binance", "BTCUSDT", 5000) is None
