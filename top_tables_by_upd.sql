WITH q_top_tables_by_block_io AS (
  SELECT
    relid,
    relid::regclass as tbl,
    current_setting( 'block_size' )::numeric *  coalesce(heap_blks_read, 0)
      + coalesce(idx_blks_read, 0) + coalesce(toast_blks_read, 0) + coalesce(tidx_blks_read, 0) as total_block_read_io_b
  FROM
    pg_statio_user_tables
  WHERE
    NOT schemaname LIKE E'pg\\_temp%'
  ORDER BY coalesce(heap_blks_read, 0) + coalesce(idx_blks_read, 0) + coalesce(toast_blks_read, 0) + coalesce(tidx_blks_read, 0) DESC
  LIMIT 20
)
SELECT
--   now(),
--   ut.relid,
  ut.relid::regclass as table,
  pg_size_pretty(pg_table_size(relid)) as table_data_size,
  pg_size_pretty(pg_indexes_size(relid)) as indexes_size,
  pg_size_pretty(total_block_read_io_b) as total_block_read_io,
  (n_tup_upd / 1e6)::int as n_tup_upd_m,
  (n_tup_hot_upd / 1e6)::int as n_tup_hot_upd_m,
  (n_tup_newpage_upd / 1e6)::int as n_tup_newpage_upd_m,
  seq_scan,
  (idx_scan / 1e6)::int as idx_scan_m
FROM
  pg_stat_user_tables ut
  JOIN q_top_tables_by_block_io q USING (relid)
WHERE
  pg_relation_size(ut.relid) > 1e9  -- >1GB
ORDER BY n_tup_upd_m DESC
LIMIT 10 ;
