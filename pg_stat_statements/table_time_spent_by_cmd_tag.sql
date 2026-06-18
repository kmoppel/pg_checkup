with q_grand_totals as (
  select
  sum(total_exec_time) as grand_total_exec_time,
  sum(blk_read_time) as grand_blk_read_time,
  sum(blk_write_time) as grand_blk_write_time
  from pg_stat_statements
  where calls > 100
  and dbid = (select oid from pg_database where datname = current_database())
)
select
  cmd_tag,
  sum(total_exec_time) as total_exec_time,
  (100::numeric * sum(total_exec_time) / max(grand_total_exec_time))::numeric(4,1) as pct_of_grand_total_exec_time,
  sum(blk_read_time)::int8 as blk_read_time,
  (100::numeric * sum(blk_read_time) / max(grand_blk_read_time))::numeric(4,1) as pct_of_grand_blk_read_time,
  sum(blk_write_time)::int8 as blk_write_time,
  (100::numeric * sum(blk_write_time) / max(grand_blk_write_time))::numeric(4,1) as grand_blk_write_time
from (

select calls, total_exec_time, blk_read_time, blk_write_time, split_part(query, ' ', 1) as cmd_tag, query
from (
  select calls, total_exec_time,
  blk_read_time, blk_write_time,
  regexp_replace( query, '^\\*.*\*/ ', '') as query
  from pg_stat_statements
  where query ~* '"xxx"'  -- put table name here
  and calls > 100
) x
) y, q_grand_totals
group by cmd_tag ;
