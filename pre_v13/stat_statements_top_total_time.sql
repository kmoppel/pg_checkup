-- more exact (stats_reset() safe + accounts also for statements that might have been already removed due to "pg_stat_statements.max" kicking in)
-- ...but could get slow for very large snapshots


--explain analyze
select
  queryid,
  round(sum(total_time - lag_total_time)::numeric, 1) as total_time_ms,
  sum(calls - lag_calls) as calls,
  round(avg( (total_time - lag_total_time) / (calls - lag_calls) )::numeric, 2) as mean_time_ms,
  max(query::varchar(100)) as query_truncated,
  array_agg(distinct coalesce(rolname::text, userid::text)) as users
from (

	select
	  queryid,
	  userid,
	  total_time, lag(total_time) over w as lag_total_time,
	  calls, lag(calls) over w as lag_calls,
	  query
	from
	  ss_snaps
	window w as (partition by queryid, userid order by now)

) x
left join
  pg_roles on oid = userid /* left join as user might be dropped during stats gathering */
where 
  total_time > lag_total_time
  and calls > lag_calls
group by
  queryid
order by
  total_time_ms desc
limit
  10;
