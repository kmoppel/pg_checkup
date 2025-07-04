select
  queryid,
  sum(s.calls)::int8 / 1000 as calls_k,
  avg(s.mean_exec_time)::numeric(12,1) as exec_ms,
  avg(s.stddev_exec_time)::numeric(12,1) as stddev_ms,
  (sum(s.total_exec_time) / 1000)::int8 as total_s,
  (100.0 * sum(s.total_exec_time) / (select sum(total_exec_time) from pg_stat_statements))::numeric(4,1) as total_pct,
  (100.0 * sum(shared_blks_hit)::numeric / (sum(shared_blks_read) + sum(shared_blks_hit)))::int as cache_ratio,
  max(ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g')))::varchar(200) as query
from
  pg_stat_statements s
where
  calls > 5
  and dbid = (select oid from pg_database where datname = current_database())
  and not upper(s.query) like any (array['DEALLOCATE%', 'SET %', 'RESET %', 'BEGIN%', 'BEGIN;',
    'COMMIT%', 'END%', 'ROLLBACK%', 'SHOW%'])
group by
  queryid
having
  sum(shared_blks_read) + sum(shared_blks_hit) > 0
order by
  sum(total_exec_time) desc
limit
  5;
