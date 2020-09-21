select
  queryid,
  array_to_string(array_agg(distinct quote_ident(pg_get_userbyid(userid))), ',') as users,
  sum(s.calls)::int8 as calls,
  round(avg(s.mean_time)::numeric, 3) as mean_time,
  round(sum(s.total_time)::numeric, 3)::double precision as total_time,
  round(100 * sum(s.total_time)::numeric / (select sum(total_time)::numeric from pg_stat_statements where dbid = (select oid from pg_database where datname = current_database())), 1)::double precision as total_time_approx_pct,
  sum(shared_blks_hit)::int8 as shared_blks_hit,
  sum(shared_blks_read)::int8 as shared_blks_read,
  sum(shared_blks_written)::int8 as shared_blks_written,
  sum(shared_blks_dirtied)::int8 as shared_blks_dirtied,
  sum(temp_blks_read)::int8 as temp_blks_read,
  sum(temp_blks_written)::int8 as temp_blks_written,
  round(sum(blk_read_time)::numeric, 3)::double precision as blk_read_time,
  round(sum(blk_write_time)::numeric, 3)::double precision as blk_write_time,
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
order by
  total_time desc
limit
  10;
