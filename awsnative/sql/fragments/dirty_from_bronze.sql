-- The (instrument_id, day) partitions the Gold rebuild must touch, for the
-- micro-batch writer. No trailing semicolon: this is a CTE body, substituted
-- into the dirty_cte slot of merge_gold_bars_1m.sql. (Naming that slot with
-- real placeholder syntax here would make the renderer try to fill it.)
--
-- Spec D3 says the dirty set is "a return value passed between two steps".
-- Athena's MERGE reports no affected rows, so there is nothing to return. It is
-- derived here instead, from exactly the same bounded Bronze window the Silver
-- merge read -- which yields the same set with no state travelling between
-- state-machine states.
--
-- It stays a fragment because D3's real invariant is that every writer shares
-- ONE bars computation. Stage N4's backfill renders merge_gold_bars_1m.sql with
-- a dirty_cte over archive_staging instead, and the aggregation below it does
-- not change. Adding a writer means adding a fragment, not forking the merge.
--
-- The epoch bound is a sanity floor, NOT the validity contract (that is
-- fragments/valid_trade.sql). Without it a quarantined row carrying a garbage
-- event_ts nominates a partition holding no Silver rows at all: harmless, but
-- it makes the Gold merge scan for nothing.
SELECT DISTINCT
    b.instrument_id,
    CAST(from_unixtime(b.event_ts_us / 1000000.0) AS DATE) AS dt
FROM ${database}.bronze_trades_stream b
WHERE b.ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')
  AND b.event_ts_us BETWEEN 1500000000000000 AND 1900000000000000
