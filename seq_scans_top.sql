select
  format('%I.%I', schemaname, c.relname) as table,
  seq_scan,
  seq_tup_read / seq_scan as avg_rows_per_scan,
  -- uncomment if FS slow...
  pg_size_pretty(pg_relation_size(relid)) as table_size
from
  pg_stat_user_tables
  join
  pg_class c on c.oid = relid
where
  pg_relation_size(relid) > 1e8 /* > 100mb tbls. uncomment if FS slow... */
  and seq_scan > 0
order by
  avg_rows_per_scan desc
limit
  10;
