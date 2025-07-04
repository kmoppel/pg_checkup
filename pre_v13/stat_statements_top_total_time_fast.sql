-- Not stats_reset() safe!
create index if not exists ss_snaps_ind1 ON ss_snaps (queryid, userid, now);


--explain analyze
select
  *,
  round(total_time_ms * 100 / (sum(total_time_ms) over ()), 1)::text || ' %' as from_global_total
from (

select
  queryid,
  max(date_trunc('second', "to" - "from")) as stats_window,
  round(sum(total_time::numeric), 1) as total_time_ms,
  sum(calls) as calls,
  round(sum(total_time::numeric) / sum(calls), 2) as mean_time_ms,  
  array_agg(distinct coalesce(rolname::text, userid::text)) as users,
  max(query::varchar(100)) as query_truncated
from (

select
  t2.userid,
  t2.queryid,
  t2.total_time - t1.total_time as total_time,
  t2.calls - t1.calls as calls,
  t2.query,
  t1.now as "from",
  t2.now as "to"
from
  ss_snaps as t2
join lateral (
  select * from ss_snaps t1
  where t1.queryid = t2.queryid and t1.userid = t2.userid
  and t2.total_time != t1.total_time
  and t2.calls != t1.calls
  order by now
  limit 1
  ) t1 on true
where t2.now = (select max(now) from ss_snaps)

) x
left join
  pg_roles on oid = userid /* left join as user might be dropped during stats gathering */
group by
  queryid

) y
order by
  total_time_ms desc
limit
  10;
