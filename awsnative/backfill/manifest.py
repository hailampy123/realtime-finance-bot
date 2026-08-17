"""Work items and outcomes for one backfill run. Pure.

TWO SHAPES, ONE PURPOSE. A `WorkItem` is what Step Functions' Distributed Map
iterates; an `Outcome` is what the Lambda hands back. Both serialise to flat
string-only JSON, and that is not incidental:

  * Distributed Map's JSON Lines reader does not coerce types. A Lambda that gets
    an int where it expected a string fails deep inside a format string rather
    than at the boundary, so everything crossing the boundary is a string.
  * `ResultWriter` collects every Outcome into S3 objects that Athena then reads
    as one external table. Flat rows of strings are exactly what a CSV-like
    external table can describe.

WHY OUTCOMES ARE NOT WRITTEN ONE AT A TIME. The obvious design has the Lambda
update `backfill_manifest` itself, which is one Athena `MERGE` per archive file:
223 round trips for a two-year deep tier, each costing more wall-clock than the
download it records. Instead the map collects outcomes to S3 and a single `MERGE`
at the end loads all of them. Four Athena statements per run, whatever the file
count.

STATUS, and the one distinction that matters (spec §6.2). `SKIPPED_NO_DATA` is
not a failure. A symbol that did not trade on a given day has no archive file,
and marking that FAILED would make a clean backfill look broken while hiding the
runs that really did fail.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from awsnative.backfill.tiers import ARCHIVE_BASE_URL, ArchiveFile, Granularity, Tier

STAGING_PREFIXES = {
    Tier.DEEP: "archive_staging_klines",
    Tier.HOT: "archive_staging_trades",
}


class Status(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    SKIPPED_NO_DATA = "SKIPPED_NO_DATA"


@dataclass(frozen=True, slots=True)
class WorkItem:
    """One archive file to fetch. The dict form IS the Lambda's input contract."""

    tier: Tier
    venue: str
    instrument_id: str
    venue_symbol: str
    granularity: Granularity
    period: str
    dt: str
    archive_key: str
    staging_key: str

    @property
    def url(self) -> str:
        return f"{ARCHIVE_BASE_URL}/{self.archive_key}"

    @property
    def checksum_url(self) -> str:
        return f"{self.url}.CHECKSUM"

    @property
    def filename(self) -> str:
        return self.archive_key.rsplit("/", 1)[-1]

    def to_json(self) -> dict[str, str]:
        return {
            "tier": self.tier.value,
            "venue": self.venue,
            "instrument_id": self.instrument_id,
            "venue_symbol": self.venue_symbol,
            "granularity": self.granularity.value,
            "period": self.period,
            "dt": self.dt,
            "archive_key": self.archive_key,
            "staging_key": self.staging_key,
        }

    @classmethod
    def from_json(cls, payload: dict[str, str]) -> WorkItem:
        return cls(
            tier=Tier(payload["tier"]),
            venue=payload["venue"],
            instrument_id=payload["instrument_id"],
            venue_symbol=payload["venue_symbol"],
            granularity=Granularity(payload["granularity"]),
            period=payload["period"],
            dt=payload["dt"],
            archive_key=payload["archive_key"],
            staging_key=payload["staging_key"],
        )


@dataclass(frozen=True, slots=True)
class Outcome:
    """What happened to one WorkItem. Loaded into backfill_manifest in bulk."""

    archive_key: str
    status: Status
    sha256_actual: str
    row_count: int
    error: str

    @classmethod
    def done(cls, *, archive_key: str, sha256: str, row_count: int) -> Outcome:
        return cls(
            archive_key=archive_key,
            status=Status.DONE,
            sha256_actual=sha256,
            row_count=row_count,
            error="",
        )

    @classmethod
    def skipped_no_data(cls, *, archive_key: str, reason: str) -> Outcome:
        return cls(
            archive_key=archive_key,
            status=Status.SKIPPED_NO_DATA,
            sha256_actual="",
            row_count=0,
            error=reason,
        )

    @classmethod
    def failed(cls, *, archive_key: str, reason: str) -> Outcome:
        return cls(
            archive_key=archive_key,
            status=Status.FAILED,
            sha256_actual="",
            row_count=0,
            error=reason,
        )

    def to_json(self) -> dict[str, str]:
        return {
            "archive_key": self.archive_key,
            "status": self.status.value,
            "sha256_actual": self.sha256_actual,
            "row_count": str(self.row_count),
            "error": self.error,
        }


def staging_key_for(item: ArchiveFile | WorkItem) -> str:
    """Where the loader writes this file's normalised rows.

    Flat, with no partition columns. Staging is transient: the state machine
    clears the prefix before the map runs, so it holds exactly one run's data and
    the merges read all of it. Partitioning it would add a projection
    configuration that buys nothing over a prefix that is emptied anyway.
    """
    prefix = STAGING_PREFIXES[item.tier]
    return f"{prefix}/{item.instrument_id}-{item.period}.csv.gz"


def plan(files: list[ArchiveFile], *, already_done: frozenset[str]) -> list[WorkItem]:
    """Work items for `files`, minus anything already recorded DONE.

    `already_done` is a set of archive keys, read from `backfill_manifest` in one
    query before the run starts. The checksum that made those rows trustworthy
    was verified when they were written (spec §6.2), which is what makes skipping
    them safe rather than optimistic.
    """
    return [
        WorkItem(
            tier=f.tier,
            venue=f.venue,
            instrument_id=f.instrument_id,
            venue_symbol=f.venue_symbol,
            granularity=f.granularity,
            period=f.period,
            dt=f.dt.isoformat(),
            archive_key=f.key,
            staging_key=staging_key_for(f),
        )
        for f in files
        if f.key not in already_done
    ]
