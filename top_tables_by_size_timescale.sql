with recursive /* pgwatch_generated */
    q_root_part as (
        select c.oid,
               c.relkind,
               exists (select * from timescaledb_information.hypertables where hypertable_schema = n.nspname and hypertable_name = c.relname) as is_hyper,
               n.nspname root_schema,
               c.relname root_relname
        from pg_class c
                 join pg_namespace n on n.oid = c.relnamespace
        where relkind in ('p', 'r')
          and relpersistence != 't'
          and not n.nspname like any (array[E'pg\\_%', 'information_schema', E'\\_timescaledb%'])
          and not exists(select * from pg_inherits where inhrelid = c.oid)
          and exists(select * from pg_inherits where inhparent = c.oid)
    ),
    q_parts (relid, relkind, is_hyper, level, root) as (
        select oid, relkind, is_hyper, 1, oid
        from q_root_part
        union all
        select inhrelid, c.relkind, is_hyper, level + 1, q.root
        from pg_inherits i
                 join q_parts q on inhparent = q.relid
                 join pg_class c on c.oid = i.inhrelid
    ),
    q_tstats as (
        select relid,
               c.relkind,
               quote_ident(schemaname) as table_schema,
               quote_ident(schemaname) || '.' || quote_ident(ut.relname)                                as table_name,
               exists (select * from timescaledb_information.hypertables where hypertable_schema = ut.schemaname and hypertable_name = ut.relname) as is_hyper,               
               pg_table_size(relid)                                                                     as table_size_b,
               pg_total_relation_size(relid)                                                            as total_relation_size_b,
               case when c.reltoastrelid != 0 then pg_total_relation_size(c.reltoastrelid) else 0::int8 end as toast_size_b,
               seq_scan,
               seq_tup_read,
               coalesce(idx_scan, 0) as idx_scan,
               coalesce(idx_tup_fetch, 0) as idx_tup_fetch,
               case when c.relkind != 'p' then age(c.relfrozenxid) else 0 end as tx_freeze_age,
               last_seq_scan,
               n_tup_ins,
               n_tup_upd,
               n_tup_del,
               n_tup_hot_upd,
               n_live_tup,
               n_dead_tup
        from pg_stat_user_tables ut
            join pg_class c on c.oid = ut.relid
            left join pg_class t on t.oid = c.reltoastrelid
            left join pg_index ti on ti.indrelid = t.oid
            left join pg_class tir on tir.oid = ti.indexrelid
        where
          -- leaving out fully locked tables as pg_relation_size also wants a lock and would wait
          not exists (select 1 from pg_locks where relation = relid and mode = 'AccessExclusiveLock')
          and c.relpersistence != 't' -- temp tables
        order by case when c.relkind = 'p' then 1e9::int else coalesce(c.relpages, 0) + coalesce(t.relpages, 0) + coalesce(tir.relpages, 0) end desc
        limit 1500 /* NB! When changing the bottom final LIMIT also adjust this limit. Should be at least 5x bigger as approx sizes depend a lot on vacuum frequency.
                    The general idea is to reduce filesystem "stat"-ing on tables that won't make it to final output anyways based on approximate size */
    ),
    q_db_size as materialized (
        select pg_database_size(current_database()) as db_size_b
    )
/* plain unpartitioned tables */
select
    table_name,
    false as is_part,
    is_hyper,
    pg_size_pretty(total_relation_size_b) as total_size,
    total_relation_size_b as total_size_b,
    (100::numeric * total_relation_size_b / db_size_b)::numeric(4,1)  as pct_of_db_size,
    pg_size_pretty(table_size_b) as table_size,
    -- table_size_b,
    pg_size_pretty(toast_size_b) as toast_size,
    -- toast_size_b,
    seq_scan,
    -- seq_tup_read,
    -- idx_scan,
    -- idx_tup_fetch,
    (tx_freeze_age / 1e6)::int as tx_freeze_age_m,
    (n_tup_ins / 1e6)::int8 as tup_ins_m,
    (n_tup_upd / 1e6)::int8 as tup_upd_m,
    (n_tup_del / 1e6)::int8 as tup_del_m,
    -- n_tup_hot_upd,
    (n_live_tup / 1e6)::int8 as live_tup_m,
    -- n_dead_tup
    case when n_live_tup = 0 then 0 else (100::numeric * n_dead_tup / n_live_tup)::numeric(5,1) end as dead_tup_pct,
    last_seq_scan
from q_tstats, q_db_size
where not table_schema like E'\\_timescaledb%'
and not exists (select * from pg_inherits where inhrelid = q_tstats.relid)

union all

/* partitioned tables (both pg / timescale) */
select * from (
    select
        quote_ident(qr.root_schema) || '.' || quote_ident(qr.root_relname) as table_name,
        true as is_part,
        qr.is_hyper,
        pg_size_pretty(sum(total_relation_size_b)::int8) as total_size,
        sum(total_relation_size_b)::int8 as total_size_b,
        (100::numeric * sum(total_relation_size_b) / max(db_size_b))::numeric(4,1)  as pct_of_db_size,
        pg_size_pretty(sum(table_size_b)::int8) as table_size,
        -- sum(table_size_b)::int8 table_size_b,
        pg_size_pretty(sum(toast_size_b)::int8) as toast_size,
        -- sum(toast_size_b)::int8 toast_size_b,
        sum(seq_scan)::int8 seq_scan,
        -- sum(seq_tup_read)::int8 seq_tup_read,
        -- sum(idx_scan)::int8 idx_scan,
        -- sum(idx_tup_fetch)::int8 idx_tup_fetch,
        (max(tx_freeze_age)::int8 / 1e6)::int tx_freeze_age_m,
        (sum(n_tup_ins) / 1e6)::int8 tup_ins_m,
        (sum(n_tup_upd) / 1e6)::int8 tup_upd_m,
        (sum(n_tup_del) / 1e6)::int8 tup_del_m,
        -- sum(n_tup_hot_upd)::int8 n_tup_hot_upd,
        (sum(n_live_tup) / 1e6)::int8 live_tup_m,
        -- sum(n_dead_tup)::int8 n_dead_tup
        case when sum(n_live_tup) = 0 then 0 else (100::numeric * sum(n_dead_tup)::int8 / sum(n_live_tup)::int8)::numeric(5,1) end as dead_tup_pct,
        max(last_seq_scan) as last_seq_scan        
      from
           q_tstats ts
           join q_parts qp on qp.relid = ts.relid
           join q_root_part qr on qr.oid = qp.root
           join q_db_size on true
      group by
           1, 2, 3
) x
order by total_size_b desc nulls last
limit 10;