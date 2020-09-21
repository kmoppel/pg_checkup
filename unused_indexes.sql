SELECT
  *,
  pg_size_pretty(index_size) as index_size_pretty,
  round(100 * index_size / total_unused_index_size::numeric, 1) as pct_of_total_unused
FROM (
SELECT
  *,
  sum(index_size) over() as total_unused_index_size
FROM (
SELECT format('%I.%I', s.schemaname, s.relname) AS tablename,
       format('%I.%I', s.schemaname, s.indexrelname) AS indexname,
       idx_scan,
       pg_size_pretty(pg_table_size(s.relid)) as table_size,
       pg_relation_size(s.indexrelid) AS index_size
FROM pg_catalog.pg_stat_user_indexes s
   JOIN pg_catalog.pg_index i ON s.indexrelid = i.indexrelid
WHERE s.idx_scan = 0      -- has never been scanned
  AND 0 <>ALL (i.indkey)  -- no index column is an expression
  AND NOT EXISTS          -- does not enforce a constraint
         (SELECT 1 FROM pg_catalog.pg_constraint c
          WHERE c.conindid = s.indexrelid)
) x
) y
ORDER BY index_size DESC, tablename, indexname;
