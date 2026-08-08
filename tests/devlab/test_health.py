from __future__ import annotations

from devlab.health import sequence_gaps
from tests.devlab.conftest import record


def test_a_contiguous_run_has_no_gaps():
    records = [record(trade_id=str(i), sequence=i) for i in range(1, 6)]
    report = sequence_gaps(records)
    assert report.gaps == []
    assert report.checked == 5


def test_a_missing_sequence_is_reported():
    records = [record(trade_id="1", sequence=1), record(trade_id="2", sequence=5)]
    report = sequence_gaps(records)
    assert len(report.gaps) == 1
    assert report.gaps[0].last_seen == 1
    assert report.gaps[0].next_seen == 5
    assert report.missing == 3


def test_sequences_are_tracked_per_symbol():
    # Two symbols interleaved, each contiguous on its own. Tracking them
    # together would invent a gap that is not there.
    records = [
        record(venue_symbol="BTCUSDT", trade_id="1", sequence=1),
        record(venue_symbol="ETHUSDT", trade_id="2", sequence=900),
        record(venue_symbol="BTCUSDT", trade_id="3", sequence=2),
        record(venue_symbol="ETHUSDT", trade_id="4", sequence=901),
    ]
    assert sequence_gaps(records).gaps == []


def test_coinbase_is_skipped_rather_than_reported_clean():
    # Coinbase's sequence_num is connection-wide and the topic is partitioned
    # by symbol, so any "gap" found here would be a read artefact.
    records = [
        record(venue="coinbase", venue_symbol="BTC-USD", trade_id="1", sequence=1),
        record(venue="coinbase", venue_symbol="BTC-USD", trade_id="2", sequence=99),
    ]
    report = sequence_gaps(records)
    assert report.gaps == []
    assert report.checked == 0
    assert report.skipped_venues == ["coinbase"]


def test_mixed_venues_check_only_the_checkable_one():
    records = [
        record(venue="binance", trade_id="1", sequence=1),
        record(venue="coinbase", venue_symbol="BTC-USD", trade_id="2", sequence=500),
        record(venue="binance", trade_id="3", sequence=4),
    ]
    report = sequence_gaps(records)
    assert report.checked == 2
    assert report.missing == 2
    assert report.skipped_venues == ["coinbase"]


def test_null_sequences_are_ignored():
    records = [record(trade_id="1", sequence=None), record(trade_id="2", sequence=None)]
    report = sequence_gaps(records)
    assert report.checked == 0
    assert report.gaps == []


def test_an_empty_read_is_not_an_error():
    report = sequence_gaps([])
    assert (report.checked, report.gaps, report.missing) == (0, [], 0)
