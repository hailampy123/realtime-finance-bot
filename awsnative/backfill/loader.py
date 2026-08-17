"""The archive loader. The only module here that touches the network.

Spec §6.2: "Lambda does I/O only -- download, verify checksum, unzip, parse,
write ... All merging is Athena SQL against a staging external table. That keeps
exactly one transform engine and makes the parsers unit-testable offline against
golden fixtures."

I/O is injected rather than imported, which is what lets every decision below be
asserted offline. `handler` is the only function that builds the real clients.

ONE JOB, TWO ACTIONS. `clear_staging` and `load` are both archive-staging I/O and
both belong to this function, so the state machine calls one Lambda with an
`action` field rather than deploying two. The state machine clears staging once
before the map, so a merge always reads exactly this run's data.

DEPENDENCIES ARE THE STANDARD LIBRARY PLUS BOTO3, and that is a design goal, not
an accident. Writing Parquet here would mean pyarrow -- ~90 MB, so a layer or a
container image and a build step -- for a format that is read once and deleted.
See staging.py for the full argument.

THE ERROR BOUNDARY IS DELIBERATE. A bad archive file becomes a FAILED or
SKIPPED_NO_DATA row so the map keeps going and the manifest records why. A bug in
this code propagates and fails the Lambda. Collapsing the two would file a
programming error under "the archive was bad" and send the next reader to the
wrong place.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from awsnative.backfill.checksum import ChecksumError, verify
from awsnative.backfill.epoch import TimestampUnitError
from awsnative.backfill.manifest import STAGING_PREFIXES, Outcome, WorkItem
from awsnative.backfill.parsers import parse_agg_trades, parse_klines
from awsnative.backfill.staging import klines_csv_gz, trades_csv_gz
from awsnative.backfill.tiers import Tier

_FETCH_TIMEOUT_SECONDS = 120

# Everything a bad archive file can raise. Anything outside this set is a bug and
# must not be recorded as a data problem.
_DATA_ERRORS = (ChecksumError, TimestampUnitError, ValueError, zipfile.BadZipFile)


class ArchiveNotFound(LookupError):
    """The archive has no object at this URL. Data, not an error (spec §6.2)."""


@dataclass(frozen=True, slots=True)
class Loader:
    """Fetch, verify, parse and stage one archive file.

    fetch:          url -> bytes. Raises ArchiveNotFound on 404.
    put:            (key, body) -> None.
    delete_prefix:  prefix -> number of objects removed.
    """

    fetch: Callable[[str], bytes]
    put: Callable[[str, bytes], None]
    delete_prefix: Callable[[str], int]

    def load(self, item: WorkItem) -> Outcome:
        """Stage one archive file. Never raises for a bad file; see the module docstring."""
        try:
            return self._load(item)
        except ArchiveNotFound as missing:
            return Outcome.skipped_no_data(archive_key=item.archive_key, reason=str(missing))
        except _DATA_ERRORS as bad:
            return Outcome.failed(archive_key=item.archive_key, reason=str(bad))

    def _load(self, item: WorkItem) -> Outcome:
        # Checksum first: it is a 91-byte file. An instrument that never traded on
        # a date has neither object, and paying for the zip to discover that is
        # waste multiplied by the number of files in the plan.
        checksum_text = self.fetch(item.checksum_url).decode()
        blob = self.fetch(item.url)
        digest = verify(blob, checksum_text, expected_filename=item.filename)

        lines = _sole_member_lines(blob)
        body, row_count = _stage(item, lines)

        # Written only after the digest and every row have been accepted, so a
        # staging object never holds half a file.
        self.put(item.staging_key, body)
        return Outcome.done(archive_key=item.archive_key, sha256=digest, row_count=row_count)

    def clear_staging(self) -> int:
        """Empty both staging prefixes. Returns the number of objects removed."""
        return sum(self.delete_prefix(f"{prefix}/") for prefix in STAGING_PREFIXES.values())


def _stage(item: WorkItem, lines: list[str]) -> tuple[bytes, int]:
    """Parse `lines` for the item's tier and return (staging body, row count)."""
    if item.tier is Tier.DEEP:
        bars = list(parse_klines(lines))
        return klines_csv_gz(item.instrument_id, bars), len(bars)
    trades = list(parse_agg_trades(lines))
    return trades_csv_gz(item.instrument_id, item.venue_symbol, trades), len(trades)


def _sole_member_lines(blob: bytes) -> list[str]:
    """The lines of the zip's single member.

    Every archive file inspected holds exactly one CSV. More than one means the
    format changed, and picking the first would silently drop the rest.
    """
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        members = archive.namelist()
        if len(members) != 1:
            raise ValueError(f"expected one member in the archive, found {len(members)}: {members}")
        return archive.read(members[0]).decode().splitlines()


def _http_fetch(url: str) -> bytes:
    """Fetch `url`, mapping 404 to ArchiveNotFound.

    urllib rather than httpx or requests: the Lambda then needs no dependency
    beyond boto3, which the runtime already provides.
    """
    try:
        with urlopen(url, timeout=_FETCH_TIMEOUT_SECONDS) as response:
            body: bytes = response.read()
            return body
    except HTTPError as error:
        if error.code == 404:
            raise ArchiveNotFound(f"HTTP 404 for {url}") from None
        raise
    except URLError as error:
        raise RuntimeError(f"cannot reach {url}: {error.reason}") from error


def _s3_loader(bucket: str) -> Loader:
    import boto3

    client = boto3.client("s3")

    def put(key: str, body: bytes) -> None:
        client.put_object(Bucket=bucket, Key=key, Body=body)

    def delete_prefix(prefix: str) -> int:
        removed = 0
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if not keys:
                continue
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            removed += len(keys)
        return removed

    return Loader(fetch=_http_fetch, put=put, delete_prefix=delete_prefix)


def handler(event: dict[str, Any], context: object = None) -> dict[str, str]:
    """Lambda entry point.

    Two shapes:
        {"action": "clear", "bucket": "..."}
        {"action": "load",  "bucket": "...", <WorkItem fields>}

    `load` is the default because it is what Distributed Map sends, and the map's
    items carry no `action` of their own.
    """
    bucket = event["bucket"]
    loader = _s3_loader(bucket)

    if event.get("action") == "clear":
        return {"removed": str(loader.clear_staging())}

    item = WorkItem.from_json(event)
    # The item fields travel back with the outcome so that Distributed Map's
    # ResultWriter output is a complete manifest row. The alternative is a second
    # external table over the items file and a join to recover tier and
    # instrument_id -- two more moving parts to recover data this function
    # already holds.
    return {**item.to_json(), **loader.load(item).to_json()}
