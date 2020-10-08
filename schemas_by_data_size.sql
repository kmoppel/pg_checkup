SELECT
  "schema",
  pg_size_pretty(schema_data_size) as schema_data_size
FROM (
SELECT
    quote_ident(nspname) AS "schema",
    sum(pg_table_size(c.oid)) AS schema_data_size
FROM
    pg_namespace n
    JOIN pg_class c ON c.relnamespace = n.oid
WHERE
    relkind IN ('r', 'm')
    AND relpersistence != 't'
GROUP BY
    1
) x
ORDER BY x.schema_data_size DESC;
