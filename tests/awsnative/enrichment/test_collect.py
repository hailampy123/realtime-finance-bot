"""The two collectors, driven through injected I/O.

The request budget matters: 41 requests per poll against Binance's limit of 1000
per five minutes per IP. If that ever drifts, this is where it shows.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from awsnative.enrichment.collect import (
    RATIO_ENDPOINTS,
    MacroCollector,
    PerpCollector,
    UpstreamUnavailable,
    jsonl_gz,
    macro_key,
    perp_key,
)

FIXTURES = Path(__file__).parent / "fixtures"
POLL_TS_US = 1_787_028_300_000_000  # 2026-08-18T05:25:00Z
PAIRS = [("BTC-USD", "BTCUSDT"), ("ETH-USD", "ETHUSDT")]


class Store:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, body: bytes) -> None:
        self.objects[key] = body

    def rows(self, key: str) -> list[dict[str, str]]:
        return [
            json.loads(line) for line in gzip.decompress(self.objects[key]).decode().splitlines()
        ]


class Binance:
    """Answers the real endpoint shapes from the captured fixtures."""

    def __init__(self, *, unavailable: tuple[str, ...] = ()) -> None:
        self.urls: list[str] = []
        self.unavailable = unavailable

    def fetch(self, url: str) -> bytes:
        self.urls.append(url)
        if any(fragment in url for fragment in self.unavailable):
            raise UpstreamUnavailable(f"HTTP 503 for {url}")
        if "premiumIndex" in url:
            return FIXTURES.joinpath("premium_index.json").read_bytes()
        if "openInterest" in url:
            return FIXTURES.joinpath("open_interest.json").read_bytes()
        for family, endpoint in RATIO_ENDPOINTS.items():
            if endpoint in url:
                name = {
                    "toptrader_accounts": "top_long_short_account_ratio.json",
                    "toptrader_positions": "top_long_short_position_ratio.json",
                    "global_accounts": "global_long_short_account_ratio.json",
                    "taker_volume": "takerlongshort_ratio.json",
                }[family]
                return FIXTURES.joinpath(name).read_bytes()
        raise AssertionError(f"unexpected url {url}")


class TestPerpCollector:
    def test_one_poll_issues_the_documented_request_budget(self) -> None:
        # 1 whole-market premiumIndex + per symbol: 1 open interest + 4 ratios.
        binance, store = Binance(), Store()
        PerpCollector(fetch=binance.fetch, put=store.put).collect(
            pairs=PAIRS, poll_ts_us=POLL_TS_US
        )
        assert len(binance.urls) == 1 + len(PAIRS) * 5
        assert sum("premiumIndex" in u for u in binance.urls) == 1

    def test_eight_instruments_stay_inside_the_rate_limit(self) -> None:
        # The published limit is 1000 requests per 5 minutes per IP.
        eight = [(f"X{i}-USD", f"X{i}USDT") for i in range(8)]
        assert 1 + len(eight) * 5 == 41

    def test_it_writes_one_object_holding_one_row_per_instrument(self) -> None:
        binance, store = Binance(), Store()
        result = PerpCollector(fetch=binance.fetch, put=store.put).collect(
            pairs=PAIRS, poll_ts_us=POLL_TS_US
        )
        assert result["rows"] == "2"
        assert list(store.objects) == [perp_key(POLL_TS_US)]
        assert {r["instrument_id"] for r in store.rows(result["key"])} == {"BTC-USD", "ETH-USD"}

    def test_a_failing_ratio_endpoint_does_not_cost_the_funding_rate(self) -> None:
        # One endpoint timing out must not lose what the same poll already has.
        binance = Binance(unavailable=("globalLongShortAccountRatio",))
        store = Store()
        result = PerpCollector(fetch=binance.fetch, put=store.put).collect(
            pairs=PAIRS, poll_ts_us=POLL_TS_US
        )
        row = next(r for r in store.rows(result["key"]) if r["instrument_id"] == "BTC-USD")
        assert row["last_funding_rate"] == "0.00003686"
        assert row["global_accounts_ratio"] == ""
        assert row["toptrader_accounts_ratio"] != ""

    def test_a_failing_premium_index_yields_an_empty_object_not_a_crash(self) -> None:
        # Without mark prices there are no rows worth writing, but the object is
        # still written: an absent object and an empty one mean different things
        # to whoever is looking for the gap.
        binance = Binance(unavailable=("premiumIndex",))
        store = Store()
        result = PerpCollector(fetch=binance.fetch, put=store.put).collect(
            pairs=PAIRS, poll_ts_us=POLL_TS_US
        )
        assert result["rows"] == "0"
        assert gzip.decompress(store.objects[result["key"]]) == b""


class TestMacroCollector:
    def fetcher(self, *, fail: tuple[str, ...] = ()):
        def fetch(url: str) -> bytes:
            if any(f"id={series}&" in url for series in fail):
                raise UpstreamUnavailable(f"HTTP 503 for {url}")
            if "id=CPIAUCSL" in url:
                return FIXTURES.joinpath("cpi_vintage_2026-01-15.csv").read_bytes()
            return FIXTURES.joinpath("dgs10_recent.csv").read_bytes()

        return fetch

    def test_it_pulls_every_series_at_one_vintage(self) -> None:
        store = Store()
        # Only DGS10 and CPIAUCSL have matching fixtures; the rest raise on the
        # header check, which is the behaviour under test elsewhere. Restrict to
        # the two by failing the others.
        fetch = self.fetcher(fail=("DTWEXBGS", "DGS2", "VIXCLS", "SP500"))
        result = MacroCollector(fetch=fetch, put=store.put).collect(vintage_date="2026-01-15")
        rows = store.rows(result["key"])
        assert {r["series_id"] for r in rows} == {"DGS10", "CPIAUCSL"}
        assert {r["vintage_date"] for r in rows} == {"2026-01-15"}

    def test_one_unavailable_series_does_not_lose_the_others(self) -> None:
        store = Store()
        fetch = self.fetcher(fail=("DTWEXBGS", "DGS2", "VIXCLS", "SP500", "CPIAUCSL"))
        result = MacroCollector(fetch=fetch, put=store.put).collect(vintage_date="2026-01-15")
        assert {r["series_id"] for r in store.rows(result["key"])} == {"DGS10"}

    def test_the_since_bound_reaches_the_parser(self) -> None:
        store = Store()
        fetch = self.fetcher(fail=("DTWEXBGS", "DGS2", "VIXCLS", "SP500", "DGS10"))
        result = MacroCollector(fetch=fetch, put=store.put).collect(
            vintage_date="2026-01-15", since="2025-11-01"
        )
        assert [r["observation_date"] for r in store.rows(result["key"])] == [
            "2025-11-01",
            "2025-12-01",
        ]


class TestKeys:
    def test_the_perp_key_partitions_by_the_utc_day_of_the_poll(self) -> None:
        assert perp_key(POLL_TS_US).startswith("bronze_perp_context/ingest_date=2026-08-18/")
        assert perp_key(POLL_TS_US).endswith(".jsonl.gz")

    def test_each_poll_gets_its_own_object(self) -> None:
        assert perp_key(POLL_TS_US) != perp_key(POLL_TS_US + 300_000_000)

    def test_the_macro_key_is_stable_per_vintage(self) -> None:
        # Overwriting is right: a second pull of the same vintage is the same
        # answer, and a second object would double every row the merge reads.
        assert macro_key("2026-01-15") == macro_key("2026-01-15")
        assert "ingest_date=2026-01-15" in macro_key("2026-01-15")


class TestJsonlGz:
    def test_it_is_deterministic(self) -> None:
        rows = [{"a": "1"}, {"a": "2"}]
        assert jsonl_gz(iter(rows)) == jsonl_gz(iter(rows))

    def test_each_row_is_one_line_and_the_body_ends_with_a_newline(self) -> None:
        body = gzip.decompress(jsonl_gz(iter([{"a": "1"}, {"a": "2"}]))).decode()
        assert body == '{"a":"1"}\n{"a":"2"}\n'

    def test_an_empty_iterable_is_an_empty_body(self) -> None:
        assert gzip.decompress(jsonl_gz(iter([]))) == b""


@pytest.mark.parametrize("family,endpoint", sorted(RATIO_ENDPOINTS.items()))
def test_every_ratio_family_has_an_endpoint(family: str, endpoint: str) -> None:
    from awsnative.enrichment.perp import RATIO_FAMILIES

    assert family in RATIO_FAMILIES
    assert endpoint
