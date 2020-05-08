/* some sample queries to analyze data gathered via stat_activity_monitoring.py */

-- top 5min time ranges with longest transactions
select
  to_timestamp((extract(epoch from xact_start)::int / 300)*300) as tx_start_range,
  count(*)
from (
  select pid, xact_start, max(now) - min(now) as duration from stat_activity_history where xact_start is not null group by pid, xact_start order by 3 desc limit 100
) x
group by 1
order by 2 desc
limit 10;


-- most concurrent transactions
select
  now,
  sum(count) as total_active_tx,
  string_agg(state||':'||count, ', ') as by_state
from (
select
  now,
  state,
  count(*)
from
  stat_activity_history
where
  now in (
    select
      now
    from
      stat_activity_history
    where
      xact_start is not null
    group by 1
    order by count(*) desc
    limit 10
    )
group by 1, 2
order by 1, 3 desc
) x
group by 1
order by 2 desc, 1;


-- top 5min avg tx duration
select
  to_timestamp((extract(epoch from now)::int / 300)*300) as tx_start_range,
  avg(now - xact_start) as avg_xact_duration
from
  stat_activity_history
where
  xact_start is not null
group by 1
order by 2 desc
limit 10;
