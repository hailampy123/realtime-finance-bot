-- The backfill manifest: what was fetched, whether it verified, and what it held.
--
-- This table is what makes a backfill resumable, and spec §6.2 states the reason
-- it is trustworthy enough to resume from: "Every archive file has a sibling
-- .CHECKSUM; verifying it is what makes DONE trustworthy enough to skip -- an
-- unverified skip is an assumption with a timestamp." A DONE row here means the
-- bytes were checked against the publisher's digest before any row was parsed.
--
-- Iceberg, not plain Parquet, because a re-run MERGEs new outcomes over old ones
-- for the same archive_key. That is the one property Bronze never needs (D1) and
-- the reason Silver and Gold are Iceberg too (D2).
--
-- KEYED ON archive_key, not on (venue, instrument_id, tier, dt). The key has to
-- distinguish a monthly file from the first daily file of the same month, and
-- both carry the same dt. The archive key is the publisher's own identifier and
-- is unique by construction.
--
-- TWO NARROWINGS FROM §6.2's COLUMN LIST, both because nothing writes them:
--
--   sha256_expected  dropped. The Lambda compares expected against actual before
--       it stages a single row, so a surviving row has them equal by definition.
--       On a mismatch the two digests are both in `error`, which is where you
--       would read them.
--   attempt          dropped. Step Functions' Distributed Map owns retries and
--       its execution history already holds the count. A column here would need
--       the Lambda to read the manifest to increment it -- one Athena query per
--       file, which is the cost this whole design avoids.
--
-- status, and the distinction that matters:
--   DONE             verified and staged.
--   FAILED           reached, but the digest, the zip or a row was bad. Retry.
--   SKIPPED_NO_DATA  no such object. An instrument that did not trade that day
--                    has no file, which is data rather than an error. Marking it
--                    FAILED would make a clean backfill look broken and hide the
--                    runs that really did fail.
--   PENDING/RUNNING  reserved: nothing writes them, because outcomes are loaded
--                    in bulk after the map finishes rather than transitioned
--                    per file.
CREATE TABLE IF NOT EXISTS ${database}.backfill_manifest (
    archive_key   string,
    tier          string,
    venue         string,
    instrument_id string,
    venue_symbol  string,
    granularity   string,
    period        string,
    dt            date,
    url           string,
    sha256_actual string,
    row_count     bigint,
    status        string,
    error         string,
    completed_ts  timestamp
)
PARTITIONED BY (tier)
LOCATION '${warehouse}backfill_manifest/'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'parquet',
    'write_compression' = 'snappy'
)
