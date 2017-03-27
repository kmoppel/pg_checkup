SELECT
  *,
  round(buffers / (SUM(buffers) OVER ())::numeric * 100, 1) as pct_from_total
FROM (
SELECT
  quote_ident(c.relname) ||'.'|| quote_ident(n.nspname) as relation,
  c.relkind,
  COUNT(*) AS buffers
FROM
  pg_class c
  JOIN pg_buffercache b ON b.relfilenode=c.relfilenode
  JOIN pg_database d ON b.reldatabase=d.oid
  JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE
  d.datname=current_database()
GROUP BY
  1, 2
) a
ORDER BY 3 DESC LIMIT 20