WITH q AS (
  select current_setting( 'block_size' )::numeric as bs
)
SELECT
  now(),
  pg_size_pretty(bs * (coalesce(heap_blks_read, 0) + coalesce(idx_blks_read, 0) + coalesce(toast_blks_read, 0) + coalesce(tidx_blks_read, 0))) as total_read,
  relid::regclass as table,
  relid,
  pg_size_pretty(pg_table_size(relid)) as table_data_size,
  (select seq_scan from pg_stat_user_tables where relid = pg_statio_user_tables.relid) seq_scan,
  pg_size_pretty(pg_indexes_size(relid)) as indexes_size,
  (select idx_scan from pg_stat_user_tables where relid = pg_statio_user_tables.relid) idx_scan,
  pg_size_pretty(bs * heap_blks_read) as heap_blks_read,
  (100.0::numeric * heap_blks_hit / (heap_blks_hit + heap_blks_read))::int heap_blks_hit_pct,
  pg_size_pretty(bs * idx_blks_read) as idx_blks_read,
  (100.0::numeric * idx_blks_hit / (idx_blks_hit + idx_blks_read))::int idx_blks_hit_pct,
  pg_size_pretty(bs * tidx_blks_read) as tidx_blks_read,
  (100.0::numeric * tidx_blks_hit / (tidx_blks_hit + tidx_blks_read))::int tidx_blks_hit_pct
FROM
  pg_statio_user_tables, q
WHERE
  NOT schemaname LIKE E'pg\\_temp%'
  AND coalesce(heap_blks_read, 0) + coalesce(idx_blks_read, 0) + coalesce(toast_blks_read, 0) + coalesce(tidx_blks_read, 0) > 1e6
ORDER BY coalesce(heap_blks_read, 0) + coalesce(idx_blks_read, 0) + coalesce(toast_blks_read, 0) + coalesce(tidx_blks_read, 0) DESC
LIMIT 20 ;
