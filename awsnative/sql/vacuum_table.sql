-- Generic snapshot expiry and orphan-file removal for one Iceberg table.
-- Retention is controlled by the table's own
-- vacuum_max_snapshot_age_seconds and vacuum_min_snapshots_to_keep
-- properties (spec 2026-08-17-iceberg-table-maintenance-design.md section
-- 8.3), set in the table's DDL rather than passed here.
VACUUM ${database}.${table}
