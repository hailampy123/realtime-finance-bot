-- Bronze -> Silver for macro. One row per DISTINCT value, stamped with the
-- earliest vintage that showed it.
--
-- THE PROBLEM THIS SHAPE SOLVES. The collector pulls each series' full history at
-- today's vintage, every day. A naive insert-if-absent on
-- (series_id, observation_date, vintage_date) would then store ~950 CPI rows a
-- day, almost all of them repeating a value that did not change. Within a week
-- the table is six thousand times larger than the information in it.
--
-- The fix is to insert an observation only when its VALUE is new for that series
-- and observation date. What lands is therefore one row per revision, stamped
-- with the first vintage that showed that value -- which is exactly the shape a
-- point-in-time join wants: "the latest vintage at or before as_of".
--
-- Measured on real data: 59 of 947 overlapping CPI observations changed between
-- the January and April 2026 vintages, so the steady state is a handful of rows
-- a month rather than thousands a day.
--
-- INSERT-ONLY, NO UPDATE BRANCH. A revision is a new row. An UPDATE would
-- overwrite the older value and destroy precisely the history the point-in-time
-- join reads -- the one thing this table exists to preserve.
--
-- knowledge_ts_us IS THE MIDNIGHT AFTER THE VINTAGE. A vintage published at some
-- hour of its day is safely knowable by the following midnight; claiming the
-- day's own start would assert knowledge up to 24 hours early. Up to a day of
-- conservatism is the right direction to be wrong in for an anti-lookahead bound.
--
-- DECIMAL, NOT DOUBLE, and that is load-bearing here rather than stylistic: the
-- NOT EXISTS below compares values for equality, and float equality on a revision
-- that moved by one thousandth (326.030 -> 326.031, a real example) is not a
-- comparison worth trusting.
MERGE INTO ${database}.silver_macro t
USING (
    SELECT
        f.series_id,
        f.observation_date,
        f.vintage_date,
        f.value,
        to_unixtime(CAST(f.vintage_date + interval '1' day AS TIMESTAMP)) * 1000000 AS knowledge_ts_us,
        'ALFRED' AS source_tier
    FROM (
        SELECT
            p.series_id,
            p.observation_date,
            p.value,
            min(p.vintage_date) AS vintage_date
        FROM (
            SELECT
                b.series_id,
                CAST(b.observation_date AS DATE)             AS observation_date,
                CAST(b.vintage_date AS DATE)                 AS vintage_date,
                try_cast(NULLIF(b.value, '') AS DECIMAL(38, 18)) AS value
            FROM ${database}.bronze_macro_observations b
            WHERE b.series_id IS NOT NULL
        ) p
        WHERE p.value IS NOT NULL
        GROUP BY p.series_id, p.observation_date, p.value
    ) f
    -- The value is new for this observation. Checked against the whole table
    -- rather than against the pulled window, because "has this series ever
    -- reported this value for this month" is the actual question.
    WHERE NOT EXISTS (
        SELECT 1
        FROM ${database}.silver_macro m
        WHERE m.series_id        = f.series_id
          AND m.observation_date = f.observation_date
          AND m.value            = f.value
    )
) s
ON  t.series_id        = s.series_id
AND t.observation_date = s.observation_date
AND t.vintage_date     = s.vintage_date
WHEN NOT MATCHED THEN
    INSERT (series_id, observation_date, vintage_date, value, knowledge_ts_us, source_tier)
    VALUES (s.series_id, s.observation_date, s.vintage_date, s.value, s.knowledge_ts_us,
            s.source_tier)
