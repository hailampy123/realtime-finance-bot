-- Rows landed per UTC day, per medallion layer. The "is it running" chart.
--
-- One row per (dt, layer). A layer that stops appearing is the signal: an empty
-- bar and an absent bar look different on the page, which is why the query emits
-- zero rows rather than a zero for a day nothing landed.
SELECT dt, layer, row_count
FROM (
    SELECT b.ingest_date AS dt, 'Bronze trades' AS layer, count(*) AS row_count
    FROM ${database}.bronze_trades_stream b
    WHERE b.ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')
    GROUP BY b.ingest_date
    UNION ALL
    SELECT date_format(CAST(s.event_ts AS DATE), '%Y-%m-%d'), 'Silver trades', count(*)
    FROM ${database}.silver_trades s
    WHERE s.event_ts >= current_date - interval '${lookback_days}' day
    GROUP BY date_format(CAST(s.event_ts AS DATE), '%Y-%m-%d')
    UNION ALL
    SELECT date_format(CAST(g.window_end_ts AS DATE), '%Y-%m-%d'), 'Gold bars', count(*)
    FROM ${database}.gold_bars_1m g
    WHERE g.window_end_ts >= current_date - interval '${lookback_days}' day
    GROUP BY date_format(CAST(g.window_end_ts AS DATE), '%Y-%m-%d')
)
ORDER BY dt, layer
