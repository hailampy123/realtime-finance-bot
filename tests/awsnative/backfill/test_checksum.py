"""Checksum verification.

Spec §6.2: "Every archive file has a sibling .CHECKSUM; verifying it is what
makes DONE trustworthy enough to skip -- an unverified skip is an assumption with
a timestamp." These tests are what make that sentence true.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from awsnative.backfill.checksum import (
    ChecksumError,
    digest_of,
    parse_checksum,
    verify,
)

FIXTURES = Path(__file__).parent / "fixtures"

PAYLOAD = b"not really a zip, but bytes are bytes"
PAYLOAD_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()


class TestParseChecksum:
    def test_it_parses_a_real_checksum_file(self) -> None:
        parsed = parse_checksum(FIXTURES.joinpath("klines_1m.zip.CHECKSUM").read_text())
        assert parsed.sha256 == "cecb517eaf2fb3814ba7b8364c3f6038d04ff96b1e83282c4d99faf94dd4b6f4"
        assert parsed.filename == "BTCUSDT-1m-2026-08-15.zip"

    def test_it_tolerates_a_trailing_newline(self) -> None:
        parsed = parse_checksum(f"{PAYLOAD_SHA256}  some.zip\n")
        assert parsed.filename == "some.zip"

    def test_it_tolerates_single_space_separation(self) -> None:
        # sha256sum writes two spaces for binary mode and one for text mode.
        # Accepting both costs nothing and removes a class of spurious failure.
        parsed = parse_checksum(f"{PAYLOAD_SHA256} some.zip")
        assert parsed.filename == "some.zip"

    @pytest.mark.parametrize(
        "text",
        [
            "",
            "\n",
            "nodigest",
            "zz  some.zip",
            f"{PAYLOAD_SHA256}",
            f"{PAYLOAD_SHA256[:32]}  a.zip",
        ],
    )
    def test_it_rejects_anything_that_is_not_a_digest_and_a_filename(self, text: str) -> None:
        with pytest.raises(ChecksumError):
            parse_checksum(text)


class TestDigestOf:
    def test_it_is_sha256_hex(self) -> None:
        assert digest_of(PAYLOAD) == PAYLOAD_SHA256


class TestVerify:
    def test_matching_bytes_return_the_actual_digest(self) -> None:
        actual = verify(PAYLOAD, f"{PAYLOAD_SHA256}  a.zip", expected_filename="a.zip")
        assert actual == PAYLOAD_SHA256

    def test_corrupted_bytes_raise(self) -> None:
        with pytest.raises(ChecksumError, match="digest"):
            verify(PAYLOAD + b"!", f"{PAYLOAD_SHA256}  a.zip", expected_filename="a.zip")

    def test_a_checksum_naming_a_different_file_raises(self) -> None:
        # The digest would still match here. The filename check is what catches a
        # mis-built URL that fetched one file's bytes and another file's checksum
        # from the same directory -- the one corruption a digest comparison alone
        # cannot see.
        with pytest.raises(ChecksumError, match="names"):
            verify(PAYLOAD, f"{PAYLOAD_SHA256}  other.zip", expected_filename="a.zip")

    def test_the_digest_is_compared_case_insensitively(self) -> None:
        actual = verify(PAYLOAD, f"{PAYLOAD_SHA256.upper()}  a.zip", expected_filename="a.zip")
        assert actual == PAYLOAD_SHA256
