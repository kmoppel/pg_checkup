SELECT
  schemaname,
  pg_size_pretty(size_b)
FROM (
SELECT
  schemaname,
  SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::BIGINT as size_b
FROM
  pg_tables
GROUP BY
  1
) a
ORDER BY
  size_b DESC
