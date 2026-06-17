select
  queryid,
  (100::numeric * ( sum(blk_read_time) + sum(blk_write_time) + sum(temp_blk_read_time) + sum(temp_blk_write_time)) /
  (select sum(coalesce(blk_read_time, 0)) + sum(coalesce(blk_write_time, 0)) + sum(coalesce(temp_blk_read_time, 0)) + sum(coalesce(temp_blk_write_time, 0)) from pg_stat_statements))::numeric(4,1) as total_io_time_pct,
  array_to_string(array_agg(distinct quote_ident(pg_get_userbyid(userid))), ',') as users,
  sum(s.calls)::int8 / 1000 as calls_k,
  avg(s.mean_exec_time)::numeric(12,1) as mean_exec_time_ms,
  avg(s.stddev_exec_time)::numeric(12,1) as stddev_exec_time_ms,
  (sum(s.total_exec_time) / 1000)::int8 as total_exec_time_s,
  (100.0 * sum(s.total_exec_time) / (select sum(total_exec_time) from pg_stat_statements))::numeric(4,1) as total_time_approx_pct,
  (100.0 * sum(shared_blks_hit)::numeric / (sum(shared_blks_read) + sum(shared_blks_hit)))::int as cache_ratio,
  sum(shared_blks_written) as shared_blks_written,
  sum(shared_blks_dirtied) as shared_blks_dirtied,
  sum(temp_blks_read) as temp_blks_read,
  sum(temp_blks_written) as temp_blks_written,
  sum(blk_read_time) / 1000 as blk_read_time_s,
  sum(blk_write_time) / 1000 as blk_write_time_s,
  sum(temp_blk_read_time) / 1000 as temp_blk_read_time_s,
  sum(temp_blk_write_time) / 1000 as temp_blk_write_time_s,
  max(ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g')))::varchar(300) as query
from
  pg_stat_statements s
where
  calls > 10
  and dbid = (select oid from pg_database where datname = current_database())
  and not upper(s.query) like any (array['DEALLOCATE%', 'SET %', 'RESET %', 'BEGIN%', 'BEGIN;',
    'COMMIT%', 'END%', 'ROLLBACK%', 'SHOW%'])
group by
  queryid
having
  sum(blk_read_time) + sum(blk_write_time) + sum(temp_blk_read_time) + sum(temp_blk_write_time) > 0
order by
  sum(blk_read_time) + sum(blk_write_time) + sum(temp_blk_read_time) + sum(temp_blk_write_time) desc
limit
  10;
