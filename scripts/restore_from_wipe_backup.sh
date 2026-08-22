#!/usr/bin/env bash
# Restores Iceberg table data from a pre-wipe local Parquet backup
# (see wipe_backup_YYYYMMDD/manifest.csv) into freshly recreated,
# empty Iceberg tables.
#
# Prerequisites, in order, before running this script:
#   1. New sandbox credentials in the shell (AWS_PROFILE or raw keys).
#   2. make up-aws     (recreates the lake bucket, Glue database, Lambda, etc.
#                        -- same names as before, since they're derived from
#                        the account ID, which the wipe does not change)
#   3. make ddl-aws    (creates fresh, empty Iceberg tables)
#
# Usage: ./scripts/restore_from_wipe_backup.sh wipe_backup_20260822
#
# Why CREATE EXTERNAL TABLE with explicit columns, not CTAS or LIKE:
#   - CTAS with external_location fails on this workgroup: it enforces a
#     centralized query-result location and rejects any CTAS that overrides
#     it (confirmed live during the 2026-08-22 backup that fed this script).
#   - CREATE TABLE ... LIKE <iceberg_table> would inherit table_type=ICEBERG,
#     making Athena expect Iceberg metadata (manifests, snapshots) at
#     STAGE_URI, which is plain backed-up Parquet with none of that.
#   - A plain CREATE EXTERNAL TABLE only registers metadata pointing at
#     already-written files; it does not go through the query-result-location
#     enforcement that blocks CTAS, and it borrows the same
#     external-staging-table pattern this repo already uses in
#     awsnative/sql/ddl/041_archive_staging_klines.sql and
#     042_archive_staging_trades.sql.

set -euo pipefail

BACKUP_DIR="${1:?usage: $0 <backup-dir>}"
MANIFEST="$BACKUP_DIR/manifest.csv"
[ -f "$MANIFEST" ] || { echo "no manifest at $MANIFEST" >&2; exit 1; }

TF_DB=$(terraform -chdir=infra/envs/native output -raw glue_database)
TF_WG=$(terraform -chdir=infra/envs/native output -raw athena_workgroup)
TF_BUCKET=$(terraform -chdir=infra/envs/native output -raw lake_bucket)
STAGE_PREFIX="_wipe_restore_staging"

query() {
  uv run --group awsnative python -m awsnative.query \
    --database "$TF_DB" --workgroup "$TF_WG" --sql "$1"
}

# Column definitions matching exactly what scripts/restore_from_wipe_backup.sh's
# sibling backup step wrote to Parquet: same columns as the live Iceberg
# tables, with every `timestamp` column narrowed to `timestamp` (Hive/Parquet
# external tables read millisecond precision as plain `timestamp` -- no (3)
# suffix in DDL). Trino widens timestamp -> timestamp(6) automatically on the
# INSERT INTO ... SELECT into the Iceberg target below.
declare -A SCHEMA=(
  [silver_trades]="venue string, venue_symbol string, instrument_id string, trade_id string, event_ts timestamp, ingest_ts timestamp, event_ts_us bigint, ingest_ts_us bigint, price decimal(38,18), \`size\` decimal(38,18), side string, sequence bigint, is_backfill boolean, source string"
  [silver_trades_quarantine]="row_key string, venue string, venue_symbol string, instrument_id string, trade_id string, event_ts_us bigint, ingest_ts_us bigint, price string, \`size\` string, side string, sequence bigint, is_backfill boolean, source string, ingest_date string, quarantine_reason string, quarantined_ts timestamp"
  [gold_bars_1m]="instrument_id string, window_end_ts timestamp, \`open\` decimal(38,18), high decimal(38,18), low decimal(38,18), \`close\` decimal(38,18), volume decimal(38,18), notional decimal(38,18), buy_vol decimal(38,18), sell_vol decimal(38,18), sq_log_return double, trade_count bigint, venue_coverage int, source_tier string, updated_ts timestamp"
  [silver_perp_context]="instrument_id string, venue string, venue_symbol string, snapshot_ts timestamp, snapshot_ts_us bigint, knowledge_ts_us bigint, mark_price decimal(38,18), index_price decimal(38,18), funding_rate decimal(38,18), interest_rate decimal(38,18), next_funding_ts timestamp, open_interest decimal(38,18), toptrader_long_accounts decimal(38,18), toptrader_short_accounts decimal(38,18), toptrader_ratio_accounts decimal(38,18), toptrader_long_positions decimal(38,18), toptrader_short_positions decimal(38,18), toptrader_ratio_positions decimal(38,18), global_long_accounts decimal(38,18), global_short_accounts decimal(38,18), global_ratio_accounts decimal(38,18), taker_buy_vol decimal(38,18), taker_sell_vol decimal(38,18), taker_buy_sell_ratio decimal(38,18), source_tier string"
  [silver_macro]="series_id string, observation_date date, vintage_date date, value decimal(38,18), knowledge_ts_us bigint, source_tier string"
  [native_health_metrics]="metric_ts timestamp, table_name string, tier string, row_count bigint, file_count bigint, avg_file_size_mb double, small_file_pct double, delete_file_count bigint, snapshot_count bigint, oldest_snapshot_age_seconds bigint, freshness_lag_seconds bigint, quarantine_rate_pct double"
)

tail -n +2 "$MANIFEST" | while IFS=, read -r TABLE _; do
  LOCAL_DIR="$BACKUP_DIR/$TABLE"
  if [ ! -d "$LOCAL_DIR" ]; then
    echo "=== $TABLE: no local backup, skipping (was empty at backup time) ==="
    continue
  fi

  echo "=== Restoring $TABLE ==="
  STAGE_URI="s3://${TF_BUCKET}/${STAGE_PREFIX}/${TABLE}/"

  echo "-- uploading local backup to $STAGE_URI"
  aws s3 sync "$LOCAL_DIR" "$STAGE_URI"

  echo "-- registering staging table over uploaded Parquet"
  query "DROP TABLE IF EXISTS ${TF_DB}.restore_staging_${TABLE}"
  query "CREATE EXTERNAL TABLE ${TF_DB}.restore_staging_${TABLE} (${SCHEMA[$TABLE]}) STORED AS PARQUET LOCATION '${STAGE_URI}'"

  echo "-- reloading into the fresh Iceberg table"
  query "INSERT INTO ${TF_DB}.${TABLE} SELECT * FROM ${TF_DB}.restore_staging_${TABLE}"

  echo "-- verifying row count"
  query "SELECT count(*) AS restored_rows FROM ${TF_DB}.${TABLE}"

  echo "-- cleanup"
  query "DROP TABLE IF EXISTS ${TF_DB}.restore_staging_${TABLE}"
  echo "=== $TABLE restored ==="
done

echo "All tables restored. Run 'make maintenance-verify-aws' to confirm row/file counts."
