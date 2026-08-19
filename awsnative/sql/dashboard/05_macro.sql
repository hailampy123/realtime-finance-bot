-- The macro regime, read the way a point-in-time consumer must read it.
--
-- FILTERED ON vintage_date, NEVER ON observation_date, and that distinction is
-- the whole reason silver_macro has three key columns. An observation is stamped
-- with the period it measures; a vintage is when the number became knowable. CPI
-- for August 2025 was published in September and revised in February -- verified
-- against real ALFRED responses, where 59 of 947 overlapping observations changed
-- across that boundary. Reading observation_date <= now would hand a backtest
-- revisions that had not happened.
--
-- One row per series: the latest observation, as known at the latest vintage at
-- or before now. The correlated subquery is what picks "the value in force",
-- which for a revised series is not the same as the newest row.
SELECT
    m.series_id,
    date_format(m.observation_date, '%Y-%m-%d') AS observation_date,
    date_format(m.vintage_date, '%Y-%m-%d')     AS vintage_date,
    CAST(m.value AS DOUBLE)                     AS value
FROM ${database}.silver_macro m
WHERE m.vintage_date <= current_date
  AND m.observation_date = (
      SELECT max(i.observation_date)
      FROM ${database}.silver_macro i
      WHERE i.series_id = m.series_id AND i.vintage_date <= current_date
  )
  AND m.vintage_date = (
      SELECT max(j.vintage_date)
      FROM ${database}.silver_macro j
      WHERE j.series_id = m.series_id
        AND j.observation_date = m.observation_date
        AND j.vintage_date <= current_date
  )
ORDER BY m.series_id
