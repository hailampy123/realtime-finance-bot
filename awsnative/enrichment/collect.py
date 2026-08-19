"""The two enrichment Lambdas. The only module here that touches the network.

I/O is injected rather than imported, so the request budget, the degradation
behaviour and the S3 layout are all asserted offline.

WHY NO FIREHOSE, narrowing §6.2 of the enrichment design. That section routed the
perp poller through a second Firehose delivery stream with Direct PUT, to buffer
away a small-file problem. Measured, the problem is not there: 288 polls a day at
roughly 4 KB each is 1.2 MB a day, and the merge reads only today's partition.
Writing gzipped JSON Lines straight to S3 removes a delivery stream, its IAM
role, and the Glue table Firehose would have needed for record-format conversion
-- and keeps both Lambdas on the standard library plus boto3, with no layer and
no build step.

WHY NO API KEY ANYWHERE. ALFRED's CSV export is keyless for all six series, so the
macro collector adds no Secrets Manager secret and the parent design's §7.3
claim survives this slice intact.

DEGRADATION IS PER REQUEST, NOT PER POLL. One ratio endpoint timing out must not
cost the funding rate that the same poll already fetched. A network or HTTP error
against a per-symbol endpoint leaves that family empty and the poll continues;
anything else propagates, because a bug is not a data problem.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from awsnative.enrichment.macro import SERIES, alfred_csv_url, parse_alfred_csv
from awsnative.enrichment.perp import build_rows

FAPI_BASE = "https://fapi.binance.com"

PERP_PREFIX = "bronze_perp_context"
MACRO_PREFIX = "bronze_macro_observations"

# family -> the /futures/data endpoint that answers for it
RATIO_ENDPOINTS = {
    "toptrader_accounts": "topLongShortAccountRatio",
    "toptrader_positions": "topLongShortPositionRatio",
    "global_accounts": "globalLongShortAccountRatio",
    "taker_volume": "takerlongshortRatio",
}

# limit=2 rather than 1: the endpoints answer oldest-first on a 5-minute grid, and
# asking for two makes "take the latest" observable in the payload rather than an
# assumption about how many rows come back.
RATIO_LIMIT = 2

_FETCH_TIMEOUT_SECONDS = 20


class UpstreamUnavailable(RuntimeError):
    """One endpoint did not answer. Degrade this family, keep the poll."""


@dataclass(frozen=True, slots=True)
class PerpCollector:
    """Poll Binance perpetual context and write one Bronze object."""

    fetch: Callable[[str], bytes]
    put: Callable[[str, bytes], None]

    def collect(self, *, pairs: Sequence[tuple[str, str]], poll_ts_us: int) -> dict[str, str]:
        premium_index = self._json(f"{FAPI_BASE}/fapi/v1/premiumIndex") or []

        open_interest: dict[str, Any] = {}
        ratios: dict[str, dict[str, Any]] = {}
        for _, venue_symbol in pairs:
            interest = self._json(f"{FAPI_BASE}/fapi/v1/openInterest?symbol={venue_symbol}")
            if interest is not None:
                open_interest[venue_symbol] = interest
            families: dict[str, Any] = {}
            for family, endpoint in RATIO_ENDPOINTS.items():
                payload = self._json(
                    f"{FAPI_BASE}/futures/data/{endpoint}"
                    f"?symbol={venue_symbol}&period=5m&limit={RATIO_LIMIT}"
                )
                if payload:
                    families[family] = payload
            ratios[venue_symbol] = families

        rows = build_rows(
            pairs=pairs,
            premium_index=premium_index,
            open_interest=open_interest,
            ratios=ratios,
            poll_ts_us=poll_ts_us,
        )
        key = perp_key(poll_ts_us)
        self.put(key, jsonl_gz(row.to_json() for row in rows))
        return {"rows": str(len(rows)), "key": key}

    def _json(self, url: str) -> Any:
        """Fetch and decode, or None if the endpoint did not answer."""
        try:
            return json.loads(self.fetch(url))
        except UpstreamUnavailable:
            return None


@dataclass(frozen=True, slots=True)
class MacroCollector:
    """Pull every macro series as of one vintage and write one Bronze object."""

    fetch: Callable[[str], bytes]
    put: Callable[[str, bytes], None]

    def collect(self, *, vintage_date: str, since: str | None = None) -> dict[str, str]:
        observations = []
        for series in SERIES:
            url = alfred_csv_url(series.series_id, vintage_date)
            try:
                text = self.fetch(url).decode()
            except UpstreamUnavailable:
                # One series missing is not a reason to lose the other five. The
                # gap shows up as an absent vintage, which the as-of join reads
                # correctly as "not known yet".
                continue
            observations.extend(
                parse_alfred_csv(
                    text, series_id=series.series_id, vintage_date=vintage_date, since=since
                )
            )
        key = macro_key(vintage_date)
        self.put(key, jsonl_gz(o.to_json() for o in observations))
        return {"rows": str(len(observations)), "key": key}


def perp_key(poll_ts_us: int) -> str:
    """One object per poll, partitioned by the UTC day the poll happened."""
    stamp = datetime.fromtimestamp(poll_ts_us / 1_000_000, tz=UTC)
    return f"{PERP_PREFIX}/ingest_date={stamp:%Y-%m-%d}/perp-{poll_ts_us}.jsonl.gz"


def macro_key(vintage_date: str) -> str:
    """One object per vintage, overwritten if the same vintage is pulled twice.

    Overwriting is right: a second pull of the same vintage is the same answer,
    and a second object would double every row the merge reads.
    """
    return f"{MACRO_PREFIX}/ingest_date={vintage_date}/macro-{vintage_date}.jsonl.gz"


def jsonl_gz(rows: Any) -> bytes:
    """Compact JSON Lines, gzipped deterministically.

    mtime=0 for the reason staging.py pins it: without it two identical writes
    differ in the header, and a re-run becomes a new object.
    """
    body = "".join(f"{json.dumps(row, separators=(',', ':'))}\n" for row in rows)
    return gzip.compress(body.encode(), mtime=0)


def _http_fetch(url: str) -> bytes:
    """Fetch `url`, mapping every reachable failure to UpstreamUnavailable.

    urllib rather than httpx: the Lambda then needs no dependency beyond boto3,
    which the runtime already provides.
    """
    try:
        with urlopen(url, timeout=_FETCH_TIMEOUT_SECONDS) as response:
            body: bytes = response.read()
            return body
    except HTTPError as error:
        raise UpstreamUnavailable(f"HTTP {error.code} for {url}") from None
    except URLError as error:
        raise UpstreamUnavailable(f"cannot reach {url}: {error.reason}") from None


def _s3_put(bucket: str) -> Callable[[str, bytes], None]:
    import boto3

    client = boto3.client("s3")

    def put(key: str, body: bytes) -> None:
        client.put_object(Bucket=bucket, Key=key, Body=body)

    return put


def perp_handler(event: dict[str, Any], context: object = None) -> dict[str, str]:
    """Lambda entry point for the 5-minute perpetual context poll.

    Event: {"bucket": "...", "pairs": [["BTC-USD", "BTCUSDT"], ...]}
    """
    poll_ts_us = int(datetime.now(tz=UTC).timestamp() * 1_000_000)
    collector = PerpCollector(fetch=_http_fetch, put=_s3_put(event["bucket"]))
    pairs = [(pair[0], pair[1]) for pair in event["pairs"]]
    return collector.collect(pairs=pairs, poll_ts_us=poll_ts_us)


def macro_handler(event: dict[str, Any], context: object = None) -> dict[str, str]:
    """Lambda entry point for the daily macro pull.

    Event: {"bucket": "...", "since": "2023-01-01"}

    The vintage is today, because that is the only vintage a pull today can
    honestly claim. History is built by pulling once a day and letting the merge
    keep the revisions.
    """
    vintage_date = f"{datetime.now(tz=UTC):%Y-%m-%d}"
    collector = MacroCollector(fetch=_http_fetch, put=_s3_put(event["bucket"]))
    return collector.collect(vintage_date=vintage_date, since=event.get("since"))
