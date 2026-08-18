-- Valid rows against quarantined rows, per day. The "is anything being dropped" check.
--
-- Spec §5.4: violations are never dropped, because silent discard destroys the
-- ability to explain a gap later. This query is what makes that guarantee
-- visible rather than merely true -- a quarantine rate that jumps is a data
-- change worth reading about on the day it happens.
SELECT dt, kind, row_count
FROM (
    SELECT date_format(CAST(event_ts AS DATE), '%Y-%m-%d') AS dt,
           'Accepted' AS kind, count(*) AS row_count
    FROM ${database}.silver_trades
    WHERE event_ts >= current_date - interval '${lookback_days}' day
    GROUP BY date_format(CAST(event_ts AS DATE), '%Y-%m-%d')
    UNION ALL
    SELECT ingest_date, 'Quarantined', count(*)
    FROM ${database}.silver_trades_quarantine
    WHERE ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')
    GROUP BY ingest_date
)
ORDER BY dt, kind
