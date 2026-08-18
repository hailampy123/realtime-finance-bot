-- Bronze: raw FRED/ALFRED observations, one row per (series, observation, vintage).
--
-- Plain text for the same reason as bronze_perp_context: append-only.
--
-- ONE OBJECT PER VINTAGE, overwritten if the same vintage is pulled twice. A
-- second pull of one vintage is the same answer, and a second object would double
-- every row the merge reads.
--
-- `vintage_date` IS THE POINT OF THIS TABLE. It is the date on which ALFRED was
-- asked, and therefore the earliest date the values in that row were retrievable.
-- Macro data is revised: verified against real ALFRED responses, 59 of 947
-- overlapping CPI observations changed between the January and April 2026
-- vintages. Without this column a backtest reads revisions that had not happened.
CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.bronze_macro_observations (
    series_id        string,
    observation_date string,
    vintage_date     string,
    value            string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${warehouse}bronze_macro_observations/'
TBLPROPERTIES (
    'projection.enabled'                   = 'true',
    'projection.ingest_date.type'          = 'date',
    'projection.ingest_date.format'        = 'yyyy-MM-dd',
    'projection.ingest_date.range'         = '${projection_start_date},NOW',
    'projection.ingest_date.interval'      = '1',
    'projection.ingest_date.interval.unit' = 'DAYS',
    'storage.location.template'            = '${warehouse}bronze_macro_observations/ingest_date=$${ingest_date}/'
)
