-- Generic bin-pack compaction for one Iceberg table, gated to a partition
-- window so a pass never re-scans the whole table (spec
-- 2026-08-17-iceberg-table-maintenance-design.md section 8.2).
--
-- partition_predicate is a caller-supplied fragment, not a bare column
-- name: each table's WHERE clause differs by column name and comparison
-- shape (spec 2026-08-19-iceberg-housekeeping-monitoring-design.md section
-- 3.1). The caller renders the predicate first and splices it in -- the same
-- two-step composition fragments/dirty_from_bronze.sql uses.
OPTIMIZE ${database}.${table} REWRITE DATA USING BIN_PACK WHERE ${partition_predicate}
