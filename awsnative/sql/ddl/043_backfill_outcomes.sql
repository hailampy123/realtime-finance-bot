-- Where Distributed Map's ResultWriter lands, so one MERGE can load every outcome.
--
-- WHY THIS TABLE EXISTS. The obvious design has the Lambda update
-- backfill_manifest itself, which is one Athena MERGE per archive file: 223 round
-- trips for a two-year deep tier, each costing more wall-clock than the download
-- it records. Instead the map's ResultWriter collects every Lambda return value
-- into S3 objects, this table reads them, and merge_manifest_outcomes.sql loads
-- all of them at once. Four Athena statements per run, whatever the file count.
--
-- ResultWriter writes JSON, one array per object, under a prefix it owns. The
-- JsonSerDe reads the fields it recognises and ignores the rest, which is what
-- lets this table name only the five fields Outcome.to_json produces without
-- having to model Step Functions' own envelope.
--
-- NO PARTITIONS, and emptied per run, for the same reason as the two staging
-- tables: it holds exactly one run's results.
--
-- Column names must match the keys `awsnative/backfill/loader.handler` returns,
-- which are `WorkItem.to_json()` merged with `Outcome.to_json()`. The item fields
-- travel back with the outcome so a row here is already a complete manifest row;
-- the alternative is a second external table over the items file plus a join to
-- recover tier and instrument_id, which the Lambda already had in hand.
--
-- A JsonSerDe matches by NAME rather than by position, so a rename on either side
-- surfaces as a silently NULL column rather than as shifted data. That is why
-- tests/awsnative/test_sql_contracts.py asserts the two sets agree.
CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.backfill_outcomes (
    tier          string,
    venue         string,
    instrument_id string,
    venue_symbol  string,
    granularity   string,
    period        string,
    dt            string,
    archive_key   string,
    staging_key   string,
    status        string,
    sha256_actual string,
    row_count     string,
    error         string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'false')
LOCATION '${warehouse}_backfill/outcomes/'
