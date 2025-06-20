-- PS The command tag returned by EXECUTE is that of the prepared statement, and not EXECUTE, so accounts for prep stmts,
-- but queries with leading comments go to OTHER ...
-- PS2 in case of pg_stat_statements_info.dealloc > 0 it's approximate numbers
WITH q_stat_stmts AS (
  select
    CASE WHEN query ~* '^\s?SELECT\s?pg_' THEN 'pg_' ELSE query END, -- adivisory locks or session kills etc
    calls,
    total_exec_time,
    mean_exec_time
  from
    pg_stat_statements
  where
    calls > 10 -- only want regular / app stuff, might need an increase
),
q_cmd_tag AS (
  SELECT
    coalesce(upper((regexp_match( query, '^\s?(\w+)\s+', 'i'))[1]), 'OTHER') AS query_type,
    sum(calls) calls,
    sum(total_exec_time) total_exec_time,
    avg(mean_exec_time) mean_exec_time
  FROM
    q_stat_stmts
  GROUP BY
    1
),
q_total AS (
  SELECT sum(calls) AS grand_total FROM q_cmd_tag
),
q_stats_reset AS (
  SELECT extract(epoch from now() - stats_reset) AS seconds_from_reset from pg_stat_statements_info
)
SELECT
  ct.query_type,
  (ct.calls / 1000)::int AS calls_1k,
  (ct.calls / sr.seconds_from_reset)::numeric(9,3) AS avg_calls_per_second,
  (ct.total_exec_time / 1000)::numeric(9,1) AS total_exec_time_s,
  mean_exec_time::numeric(9,1) AS mean_exec_time_ms,
  (100.0::numeric * ct.calls / t.grand_total)::numeric(7,1) AS pct_of_total_calls
FROM
  q_cmd_tag ct, q_total t, q_stats_reset sr

UNION ALL  

SELECT
  (select 'GRAND TOTAL last ' || now()::date - stats_reset::date || ' d' from pg_stat_statements_info),
  (select sum(calls) from q_stat_stmts),
  ((select sum(calls) from q_stat_stmts) / (select seconds_from_reset from q_stats_reset))::numeric(9, 1), -- total avg calls per second
  (select sum(total_exec_time) / 1000 from q_stat_stmts)::int8,
  (select avg(mean_exec_time) from q_stat_stmts)::numeric(9,1),
  100

ORDER BY
  pct_of_total_calls DESC;
