-- CREATE EXTENSION IF NOT EXISTS pg_buffercache ;

-- From PostgreSQL 16, following functions are also available:

SELECT * FROM pg_buffercache_summary() ;

SELECT * FROM pg_buffercache_usage_counts() ORDER BY buffers DESC LIMIT 20 ;

-- Top relations in SB
SELECT
  d.datname,
  bc.oid::regclass,
  (100::numeric * buffers / (select count(*) from pg_buffercache))::numeric(5,2) as pct_from_total,
  buffers
FROM (
    SELECT
      c.oid,
      reldatabase,
      COUNT(*) AS buffers
    FROM
      pg_buffercache
      JOIN pg_class c USING (relfilenode)
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 20
) bc
JOIN pg_database d ON d.oid = bc.reldatabase
ORDER BY buffers DESC
;