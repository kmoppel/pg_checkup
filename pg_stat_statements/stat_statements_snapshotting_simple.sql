-- wants < PG17, columns local_blk_read_time and local_blk_write_time added in 17

create extension if not exists  pg_stat_statements ;


create unlogged table pg_stat_statements_snapshots as
	select now(), queryid, calls, total_plan_time, total_exec_time, mean_exec_time, stddev_exec_time, rows, shared_blks_hit, shared_blks_read,
	blk_read_time + blk_write_time as io_time, wal_bytes, jit_generation_time, ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g'))::varchar(200) as query
	from pg_stat_statements where false ;



insert into pg_stat_statements_snapshots
  select now(), queryid, calls, total_plan_time, total_exec_time, mean_exec_time, stddev_exec_time, rows, shared_blks_hit, shared_blks_read,
    blk_read_time + blk_write_time as io_time, wal_bytes, jit_generation_time, ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g'))::varchar(200) as query
  from pg_stat_statements
  where toplevel
  and mean_exec_time > 0.001 ;

-- \watch 600


-- mean_exec_time, QPS
with q as (
  select
    now,
    sum(rows) rows,
    sum(calls) calls,
    avg(mean_exec_time) mean_exec_time
  from
    pg_stat_statements_snapshots
  group by
    now
), qt as (
  select
    min(now) as t1,
    max(now) as t2,
    extract(epoch from max(now) - min(now)) as duration_s
  from q
)
select
  qt.*,
  (((select rows from q where now = qt.t2) - (select rows from q where now = qt.t1)) / qt.duration_s)::numeric(12,2) as avg_rows_s,
  (((select calls from q where now = qt.t2) - (select calls from q where now = qt.t1)) / qt.duration_s)::numeric(12,2) as avg_calls_s,
  (select avg(mean_exec_time) from q)::numeric(12,2) as avg_mean_exec_time_ms
from qt ;


-- top stmt

-- to speed up analyzes:
-- create index on pg_stat_statements_snapshots (queryid, now );

with qt as (
	select
	 max(now) as t2,
	 sum(total_exec_time) as t2_grand_total_exec_time_ms
	from
	  pg_stat_statements_snapshots
	where now = (select max(now) from pg_stat_statements_snapshots)
)
select
  (100::numeric * total_exec_time / t2_grand_total_exec_time_ms)::numeric(4,2) || ' %' as tot_exec_time,
  (total_exec_time / 1000)::numeric(9,1) as total_exec_time_s,
  (calls / 1000)::numeric(9,1) as calls_1k,
  mean_exec_time::numeric(9,1) as mean_time_ms,
  date_trunc('minute', "to" - "from") as stats_window,
  queryid,
  query
from (

	select
	  queryid,
	  min("from") as "from",
	  max("to") as "to",
	  sum(total_exec_time) as total_exec_time,
	  sum(calls) as calls,
	  sum(total_exec_time) / sum(calls) as mean_exec_time,
	  max(query) as query
	from (

		select
		  t2.queryid,
		  t2.total_exec_time - t1.total_exec_time as total_exec_time,
		  t2.calls - t1.calls as calls,
		  t1.now as "from",
		  t2.now as "to",
		  t2.query
		from
		  pg_stat_statements_snapshots as t2
		join lateral (
		  select * from pg_stat_statements_snapshots t1
		  where t1.queryid = t2.queryid
		  and t2.calls > t1.calls
		  order by now
		  limit 1
		  ) t1 on true
		join qt on true
		where t2.now = qt.t2

	) x
	group by /* merge per user stats for a query */
	  queryid

) y
join qt on true
order by
  total_exec_time desc
limit
  10;


-- avg. query runtime "graph"

select
  -- now,
  extract(epoch from now)::int as now,
  avg(mean_exec_time)::numeric(7, 3) as avg_mean_exec_time_ms,
  sum(calls) as calls
from (

select
  now,
  queryid,
  (total_exec_time - total_exec_time_lag) / (calls - calls_lag) as mean_exec_time,
  calls - calls_lag as calls
from (

select
  now,
  lag(now) over (w) as now_lag,
  calls,
  lag(calls) over (w) as calls_lag,
  total_exec_time,
  lag(total_exec_time) over (w) as total_exec_time_lag,
  queryid,
  query
from (

	select
	  now,
	  queryid,
	  sum(total_exec_time) as total_exec_time,
	  sum(calls) as calls,
	  max(query) as query
    from pg_stat_statements_snapshots
    -- where query ~ '^UPDATE'
	group by /* merge per user stats for a query */
	  now, queryid
	order by
	  now, queryid

) x
window w as (partition by queryid order by now)
order by
  now

) y
where calls > calls_lag
) z
group by now
order by now
;