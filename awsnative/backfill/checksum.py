"""Verify an archive file against its sibling .CHECKSUM. Pure.

Spec §6.2: "Every archive file has a sibling .CHECKSUM; verifying it is what
makes DONE trustworthy enough to skip -- an unverified skip is an assumption with
a timestamp." A resumable loader that skips DONE partitions is only as
trustworthy as whatever wrote DONE, so this runs before any row is parsed.

Format, from a real file:

    cecb517eaf2fb3814ba7b8364c3f6038d04ff96b1e83282c4d99faf94dd4b6f4  BTCUSDT-1m-2026-08-15.zip

That is `sha256sum` output. Two spaces for binary mode, one for text mode; both
are accepted because tolerating them costs nothing and rejecting one would be a
failure with no cause worth reading.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

_SHA256_HEX_LENGTH = 64


class ChecksumError(ValueError):
    """A .CHECKSUM file is malformed, names another file, or does not match."""


@dataclass(frozen=True, slots=True)
class Checksum:
    sha256: str
    filename: str


def parse_checksum(text: str) -> Checksum:
    """Parse one `sha256sum`-format line into its digest and filename."""
    fields = text.strip().split()
    if len(fields) != 2:
        raise ChecksumError(
            f"expected '<sha256>  <filename>', got {text.strip()!r}"
            if text.strip()
            else "empty .CHECKSUM file"
        )
    digest, filename = fields
    if len(digest) != _SHA256_HEX_LENGTH:
        raise ChecksumError(f"digest is {len(digest)} chars, expected {_SHA256_HEX_LENGTH}")
    try:
        int(digest, 16)
    except ValueError:
        raise ChecksumError(f"digest is not hexadecimal: {digest!r}") from None
    return Checksum(sha256=digest.lower(), filename=filename)


def digest_of(data: bytes) -> str:
    """The SHA-256 of `data`, lowercase hex."""
    return hashlib.sha256(data).hexdigest()


def verify(data: bytes, checksum_text: str, *, expected_filename: str) -> str:
    """Check `data` against `checksum_text`. Returns the actual digest.

    The filename is checked as well as the digest. A digest comparison alone
    cannot see the one corruption that matters here: a mis-built URL that fetched
    one file's bytes and a sibling's .CHECKSUM from the same directory. That
    failure would otherwise read as data corruption rather than as a bug in
    tiers.py, and the fix for the two is not the same.
    """
    expected = parse_checksum(checksum_text)
    if expected.filename != expected_filename:
        raise ChecksumError(
            f".CHECKSUM names {expected.filename!r} but this is {expected_filename!r}; "
            "the URL pair is mismatched"
        )
    actual = digest_of(data)
    if actual != expected.sha256:
        raise ChecksumError(
            f"digest mismatch for {expected_filename}: "
            f"expected {expected.sha256}, got {actual} over {len(data)} bytes"
        )
    return actual
