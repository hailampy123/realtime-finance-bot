"""The loader, driven entirely through injected I/O.

The Lambda handler is the only module in the package that touches the network, so
it is also the only one that needs fakes. Everything it decides -- 404 means
skipped, a bad digest means failed, a bug means neither -- is asserted here
offline.
"""

from __future__ import annotations

import gzip
import io
import zipfile
from datetime import date
from pathlib import Path

import pytest

from awsnative.backfill.checksum import digest_of
from awsnative.backfill.loader import ArchiveNotFound, Loader
from awsnative.backfill.manifest import Status, plan
from awsnative.backfill.tiers import Tier, files_for_window

FIXTURES = Path(__file__).parent / "fixtures"
TODAY = date(2026, 8, 17)
BTC = ("BTC-USD", "BTCUSDT")


def zipped(member: str, body: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, body)
    return buffer.getvalue()


def deep_item():
    files = files_for_window([BTC], Tier.DEEP, date(2026, 8, 15), date(2026, 8, 15), today=TODAY)
    return plan(files, already_done=frozenset())[0]


def hot_item():
    files = files_for_window([BTC], Tier.HOT, date(2026, 8, 15), date(2026, 8, 15), today=TODAY)
    return plan(files, already_done=frozenset())[0]


class FakeStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deleted_prefixes: list[str] = []

    def put(self, key: str, body: bytes) -> None:
        self.objects[key] = body

    def delete_prefix(self, prefix: str) -> int:
        self.deleted_prefixes.append(prefix)
        removed = [k for k in self.objects if k.startswith(prefix)]
        for key in removed:
            del self.objects[key]
        return len(removed)


def loader_for(responses: dict[str, bytes], store: FakeStore | None = None) -> Loader:
    def fetch(url: str) -> bytes:
        try:
            return responses[url]
        except KeyError:
            raise ArchiveNotFound(f"HTTP 404 for {url}") from None

    target = store if store is not None else FakeStore()
    return Loader(fetch=fetch, put=target.put, delete_prefix=target.delete_prefix)


def archive_responses(item, csv_bytes: bytes) -> dict[str, bytes]:
    blob = zipped(item.filename.replace(".zip", ".csv"), csv_bytes)
    return {
        item.url: blob,
        item.checksum_url: f"{digest_of(blob)}  {item.filename}".encode(),
    }


class TestLoadSuccess:
    def test_a_deep_file_lands_in_the_klines_staging_prefix(self) -> None:
        item = deep_item()
        csv = FIXTURES.joinpath("klines_1m_micros.csv").read_bytes()
        store = FakeStore()
        outcome = loader_for(archive_responses(item, csv), store).load(item)

        assert outcome.status is Status.DONE
        assert outcome.row_count == 3
        assert item.staging_key in store.objects
        written = gzip.decompress(store.objects[item.staging_key]).decode()
        assert written.startswith("BTC-USD,1749945600000000,")

    def test_a_hot_file_lands_in_the_trades_staging_prefix_with_sides_resolved(self) -> None:
        item = hot_item()
        csv = FIXTURES.joinpath("agg_trades_millis.csv").read_bytes()
        store = FakeStore()
        outcome = loader_for(archive_responses(item, csv), store).load(item)

        assert outcome.status is Status.DONE
        written = gzip.decompress(store.objects[item.staging_key]).decode().splitlines()
        assert [line.rsplit(",", 1)[-1] for line in written] == ["SELL", "SELL", "BUY"]

    def test_the_recorded_digest_is_the_digest_of_the_bytes_that_were_parsed(self) -> None:
        item = deep_item()
        csv = FIXTURES.joinpath("klines_1m_micros.csv").read_bytes()
        responses = archive_responses(item, csv)
        outcome = loader_for(responses).load(item)
        assert outcome.sha256_actual == digest_of(responses[item.url])


class TestLoadSkips:
    def test_a_missing_checksum_is_skipped_no_data(self) -> None:
        # Checked first because it is the small file: a symbol that never traded
        # has neither, and paying for the zip download to find that out is waste.
        item = deep_item()
        outcome = loader_for({}).load(item)
        assert outcome.status is Status.SKIPPED_NO_DATA
        assert "404" in outcome.error

    def test_a_missing_zip_beside_a_present_checksum_is_skipped_no_data(self) -> None:
        item = deep_item()
        csv = FIXTURES.joinpath("klines_1m_micros.csv").read_bytes()
        responses = archive_responses(item, csv)
        del responses[item.url]
        outcome = loader_for(responses).load(item)
        assert outcome.status is Status.SKIPPED_NO_DATA

    def test_a_skip_writes_nothing_to_staging(self) -> None:
        store = FakeStore()
        loader_for({}, store).load(deep_item())
        assert store.objects == {}


class TestLoadFailures:
    def test_a_digest_mismatch_fails_and_writes_nothing(self) -> None:
        item = deep_item()
        csv = FIXTURES.joinpath("klines_1m_micros.csv").read_bytes()
        blob = zipped("x.csv", csv)
        store = FakeStore()
        outcome = loader_for(
            {item.url: blob, item.checksum_url: f"{'00' * 32}  {item.filename}".encode()},
            store,
        ).load(item)
        assert outcome.status is Status.FAILED
        assert "mismatch" in outcome.error
        assert store.objects == {}

    def test_a_corrupt_zip_fails(self) -> None:
        item = deep_item()
        blob = b"this is not a zip file"
        outcome = loader_for(
            {item.url: blob, item.checksum_url: f"{digest_of(blob)}  {item.filename}".encode()}
        ).load(item)
        assert outcome.status is Status.FAILED

    def test_a_row_with_the_wrong_shape_fails_with_the_parser_message(self) -> None:
        item = deep_item()
        outcome = loader_for(archive_responses(item, b"1,2,3\n")).load(item)
        assert outcome.status is Status.FAILED
        assert "12 columns" in outcome.error

    def test_a_file_mixing_timestamp_eras_fails(self) -> None:
        item = deep_item()
        mixed = (
            FIXTURES.joinpath("klines_1m_millis.csv").read_bytes()
            + FIXTURES.joinpath("klines_1m_micros.csv").read_bytes()
        )
        outcome = loader_for(archive_responses(item, mixed)).load(item)
        assert outcome.status is Status.FAILED
        assert "disagrees" in outcome.error

    def test_a_multi_member_zip_fails_rather_than_guessing_which_member(self) -> None:
        item = deep_item()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("a.csv", b"1\n")
            archive.writestr("b.csv", b"2\n")
        blob = buffer.getvalue()
        outcome = loader_for(
            {item.url: blob, item.checksum_url: f"{digest_of(blob)}  {item.filename}".encode()}
        ).load(item)
        assert outcome.status is Status.FAILED
        assert "one member" in outcome.error

    def test_an_unexpected_error_propagates_instead_of_becoming_a_failed_row(self) -> None:
        # A bug in this code is not a data problem. Recording it as FAILED would
        # file it under "the archive was bad", which sends the next person to the
        # wrong place entirely.
        item = deep_item()

        def exploding_fetch(url: str) -> bytes:
            raise MemoryError("out of memory")

        loader = Loader(fetch=exploding_fetch, put=FakeStore().put, delete_prefix=lambda p: 0)
        with pytest.raises(MemoryError):
            loader.load(item)


class TestClear:
    def test_it_empties_both_staging_prefixes(self) -> None:
        store = FakeStore()
        store.objects["archive_staging_klines/old.csv.gz"] = b"x"
        store.objects["archive_staging_trades/old.csv.gz"] = b"y"
        store.objects["silver_trades/data/keep.parquet"] = b"z"

        removed = loader_for({}, store).clear_staging()

        assert removed == 2
        assert list(store.objects) == ["silver_trades/data/keep.parquet"]
