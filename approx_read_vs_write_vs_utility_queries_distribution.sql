-- PS The command tag returned by EXECUTE is that of the prepared statement, and not EXECUTE, so accounts for prep stmts,
-- but queries with leading comments go to OTHER ...
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
)
SELECT
  ct.query_type,
  ct.calls,
  (ct.total_exec_time / 1000)::numeric(9,1) as total_exec_time_s,
  mean_exec_time::numeric(9,1) as mean_exec_time_ms,
  (100.0::numeric * ct.calls / t.grand_total)::numeric(7,1) AS percent_of_total_calls
FROM
  q_cmd_tag ct, q_total t
ORDER BY
  percent_of_total_calls DESC;
