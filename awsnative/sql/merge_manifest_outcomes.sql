-- Load one run's outcomes into backfill_manifest. One statement, any file count.
--
-- WHY THIS IS ONE MERGE AND NOT 223. The obvious design has the Lambda update the
-- manifest itself, which is one Athena MERGE per archive file. For a two-year deep
-- tier that is 223 round trips, each costing more wall-clock than the download it
-- records, and each contending for the same Iceberg commit lock -- so they would
-- also serialise a map whose entire purpose is to run in parallel. Instead
-- Distributed Map's ResultWriter collects every Lambda return value into S3,
-- backfill_outcomes reads them, and this statement loads all of them at once.
--
-- KEYED ON archive_key. It distinguishes a monthly file from the first daily file
-- of the same month, which (venue, instrument_id, tier, dt) cannot: both carry the
-- same dt. It is the publisher's own identifier and is unique by construction.
--
-- THE UPDATE BRANCH IS UNCONDITIONAL, and that is right here. A later run's
-- outcome for the same archive_key is a newer observation of the same fact: a
-- FAILED row that now verifies must become DONE, and a DONE row re-fetched with
-- --fresh must record the new digest. There is no fidelity ordering between two
-- outcomes for one file, unlike the Gold merge (merge_gold_from_archive.sql),
-- where the guard is load-bearing.
--
-- The row_number() dedupe is required rather than defensive: Trino's MERGE fails
-- when two source rows match one target row, and ResultWriter can emit the same
-- archive_key twice if Distributed Map retried an item after a transient error.
-- Ordering by status puts DONE ahead of FAILED alphabetically, so a retry that
-- eventually succeeded is what gets recorded.
MERGE INTO ${database}.backfill_manifest m
USING (
    SELECT
        r.archive_key,
        r.tier,
        r.venue,
        r.instrument_id,
        r.venue_symbol,
        r.granularity,
        r.period,
        r.dt,
        r.url,
        r.sha256_actual,
        r.row_count,
        r.status,
        r.error,
        r.completed_ts
    FROM (
        SELECT
            o.archive_key,
            o.tier,
            o.venue,
            o.instrument_id,
            o.venue_symbol,
            o.granularity,
            o.period,
            CAST(o.dt AS DATE)                          AS dt,
            'https://data.binance.vision/' || o.archive_key AS url,
            o.sha256_actual,
            CAST(o.row_count AS BIGINT)                 AS row_count,
            o.status,
            o.error,
            current_timestamp                           AS completed_ts,
            row_number() OVER (
                PARTITION BY o.archive_key
                ORDER BY o.status ASC
            ) AS rn
        FROM ${database}.backfill_outcomes o
    ) r
    WHERE r.rn = 1
) s
ON m.archive_key = s.archive_key
WHEN MATCHED THEN UPDATE SET
    tier          = s.tier,
    venue         = s.venue,
    instrument_id = s.instrument_id,
    venue_symbol  = s.venue_symbol,
    granularity   = s.granularity,
    period        = s.period,
    dt            = s.dt,
    url           = s.url,
    sha256_actual = s.sha256_actual,
    row_count     = s.row_count,
    status        = s.status,
    error         = s.error,
    completed_ts  = s.completed_ts
WHEN NOT MATCHED THEN
    INSERT (archive_key, tier, venue, instrument_id, venue_symbol, granularity,
            period, dt, url, sha256_actual, row_count, status, error, completed_ts)
    VALUES (s.archive_key, s.tier, s.venue, s.instrument_id, s.venue_symbol,
            s.granularity, s.period, s.dt, s.url, s.sha256_actual, s.row_count,
            s.status, s.error, s.completed_ts)
