set session characteristics as transaction read only;

select 'Mio Tx from last FREEZE' as check1;

select
  n.nspname||'.'||c.relname AS table_name,
  round(greatest(age(c.relfrozenxid), age(t.relfrozenxid)) / 1e6::numeric, 2) as max_age_mio
from
  pg_class c
join
  pg_namespace n on n.oid = c.relnamespace
left join
  pg_class t on c.reltoastrelid = t.oid  
where
  c.relkind IN ('r', 'm')  
order by
  2 desc
limit
  1;


select 'Avg vacuum / analyze age (>100MB tables)' as check2;

select
  avg(now() - greatest(last_vacuum, last_autovacuum)) as avg_vacuum_age,
  avg(now() - greatest(last_analyze, last_autoanalyze)) as avg_analyze_age
from
  pg_stat_user_tables
join
  pg_class on relid = oid
where
  pg_relation_size(relid) > 1e8;	-- 100mb



select 'FK-s without indexes' as check3;

/*
https://wiki.postgresql.org/wiki/Unindexed_foreign_keys
*/

WITH y AS (
    SELECT
        pg_catalog.format('%I.%I', n1.nspname, c1.relname)  AS referencing_tbl,
        pg_catalog.quote_ident(a1.attname) AS referencing_column,
        t.conname AS existing_fk_on_referencing_tbl,
        pg_catalog.format('%I.%I', n2.nspname, c2.relname) AS referenced_tbl,
        pg_catalog.quote_ident(a2.attname) AS referenced_column,
        pg_relation_size( pg_catalog.format('%I.%I', n1.nspname, c1.relname) ) AS referencing_tbl_bytes,
        pg_relation_size( pg_catalog.format('%I.%I', n2.nspname, c2.relname) ) AS referenced_tbl_bytes,
        pg_catalog.format($$CREATE INDEX ON %I.%I(%I);$$, n1.nspname, c1.relname, a1.attname) AS suggestion
    FROM pg_catalog.pg_constraint t
    JOIN pg_catalog.pg_attribute  a1 ON a1.attrelid = t.conrelid AND a1.attnum = t.conkey[1]
    JOIN pg_catalog.pg_class      c1 ON c1.oid = t.conrelid
    JOIN pg_catalog.pg_namespace  n1 ON n1.oid = c1.relnamespace
    JOIN pg_catalog.pg_class      c2 ON c2.oid = t.confrelid
    JOIN pg_catalog.pg_namespace  n2 ON n2.oid = c2.relnamespace
    JOIN pg_catalog.pg_attribute  a2 ON a2.attrelid = t.confrelid AND a2.attnum = t.confkey[1]
    WHERE t.contype = 'f'
    AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_index i
        WHERE i.indrelid = t.conrelid
        AND i.indkey[0] = t.conkey[1]
    )
)
SELECT  referencing_tbl,
        referencing_column,
        existing_fk_on_referencing_tbl,
        referenced_tbl,
        referenced_column,
        pg_size_pretty(referencing_tbl_bytes) AS referencing_tbl_size,
        pg_size_pretty(referenced_tbl_bytes) AS referenced_tbl_size,
        suggestion
FROM y
ORDER BY
    referencing_tbl_bytes DESC,
    referenced_tbl_bytes DESC,
    referencing_tbl,
    referenced_tbl,
    referencing_column,
    referenced_column
LIMIT 100;



select 'Invalid indexes' as check4;


SELECT
	schemaname||'.'||relname AS table_name,
	schemaname||'.'||indexrelname AS index_name,
	pg_size_pretty(index_size_bytes) AS index_size,
	pg_size_pretty(table_size_bytes) AS table_size
FROM
(
  SELECT quote_ident(schemaname) as schemaname,
         quote_ident(relname) as relname,
         quote_ident(indexrelname) as indexrelname,
         pg_relation_size(i.indexrelid) AS index_size_bytes,
         pg_indexes_size(i.relid) AS indexes_size_bytes,                 
         pg_relation_size(i.relid) AS table_size_bytes
  FROM pg_stat_user_indexes i
  JOIN pg_index USING(indexrelid) 
  WHERE NOT indisvalid
) a
ORDER BY
  index_size_bytes DESC
LIMIT 100;



select 'Unused indexes' as check5;

SELECT
  index_name,
  index_size,
  pct_of_tables_index_space,
  table_name,
  table_size,
  pg_size_pretty(total_marked_index_size_bytes::bigint) AS total_unused_indexes_size
FROM (
  SELECT
  *,
  pg_size_pretty(index_size_bytes) AS index_size,
  pg_size_pretty(indexes_size_bytes) AS indexes_size,
  pg_size_pretty(table_size_bytes) AS table_size,
  CASE WHEN indexes_size_bytes = 0 THEN 0 ELSE round((index_size_bytes::numeric / indexes_size_bytes::numeric)*100,1) END AS pct_of_tables_index_space,
  sum(index_size_bytes) over () AS total_marked_index_size_bytes
  FROM (
  SELECT
           quote_ident(schemaname)||'.'||quote_ident(relname) AS table_name,
           quote_ident(schemaname)||'.'||quote_ident(indexrelname) AS index_name,
           pg_relation_size(i.indexrelid) as index_size_bytes,
           pg_indexes_size(i.relid) AS indexes_size_bytes,
           pg_relation_size(i.relid) AS table_size_bytes,
           idx_scan AS scans
      FROM pg_stat_user_indexes i 
      JOIN pg_index USING(indexrelid) 
      WHERE NOT indisunique
  ) a
  WHERE index_size_bytes > 1e7 -- adjust as needed!
  AND scans <= 3	-- adjust as needed!
) b
ORDER BY
  scans,
  index_size_bytes DESC
LIMIT 100;



select 'Duplicate indexes' as check6;

SELECT
       n.nspname||'.'||ct.relname AS table_name,
       pg_size_pretty(pg_total_relation_size(ct.oid)) AS table_size,
       pg_total_relation_size(ct.oid) AS table_size_bytes,
       index_names,
       count,
       def as index_definiton
FROM (
  select regexp_replace(replace(pg_get_indexdef(i.indexrelid),c.relname,'X'), '^CREATE UNIQUE','CREATE') as def,
         max(indexrelid) as indexrelid,
         max(indrelid) as indrelid,
         count(1),
         array_agg(relname::text) as index_names
    from pg_index i
    join pg_class c
      on c.oid = i.indexrelid
   where indisvalid
   group 
      by regexp_replace(replace(pg_get_indexdef(i.indexrelid),c.relname,'X'), '^CREATE UNIQUE','CREATE')
  having count(1) > 1
) a
JOIN
  pg_class ci ON ci.oid=a.indexrelid        
JOIN
  pg_class ct ON ct.oid=a.indrelid
JOIN
  pg_namespace n ON n.oid=ct.relnamespace
ORDER BY count DESC, table_size_bytes DESC, table_name
LIMIT 100;


select 'Inactive replication slots' as check7;

SELECT slot_name FROM pg_replication_slots WHERE NOT active;
