select
  pid, 
  usename, 
  date_trunc('second', now() - xact_start) as xact_age,
  date_trunc('second', now() - query_start) as query_age,
  CASE WHEN wait_event_type IN ('LWLock', 'Lock', 'BufferPin') THEN true ELSE false END AS blocked,
  ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g'))::varchar(120) || '...' as query
from
  pg_stat_activity
where
  state != 'idle'
  and pid != pg_backend_pid()
  and (datname = current_database() or datname is null)
order by
  now() - query_start desc;
