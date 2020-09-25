#!/bin/bash

export PGHOST=localhost
export PGUSER=postgres
export PGPORT=5432

DO_COMPACT=0

STATEMENT_TIMEOUT=10min  # every bloat check will be a full scan if autovacuum hasnt visited the table recently
VACUUM_FULL_TIMEOUT=60min
MIN_TABLE_SIZE_TO_ANALYZE_MB=100
MIN_BLOAT_PCT_TO_REPORT=50


SQL_ALL_DBS="select datname from pg_database where not datistemplate order by 1"
ALL_DBS=$(psql -XAtq -c "$SQL_ALL_DBS" template1)

SQL_CREATE_PGSTATTUPLE="create extension if not exists pgstattuple"
SQL_TABLES="select format('%I.%I', nspname, c.relname) from pg_stat_user_tables u join pg_class c on c.oid = u.relid join pg_namespace n on n.oid = c.relnamespace where relpersistence != 't' and pg_table_size(relid) > 1e6 *  $MIN_TABLE_SIZE_TO_ANALYZE_MB order by pg_table_size(relid) desc"
# should also take fillfactor into account when setting lower to 80 or 90
# (select (regexp_matches(relopt, '[0-9]+'))[1] from unnest(c.reloptions) relopt where relopt ~ 'fillfactor') ff

i=0

for db in $ALL_DBS; do
    echo ""
    echo "*** Processing DB $db ... ***"
    
    #echo "$SQL_CREATE_PGSTATTUPLE"
    psql -XAtq -c "$SQL_CREATE_PGSTATTUPLE" "$db" &>/tmp/bloated_tables.log
    if [ $? -ne 0 ]; then
        echo "error on '$SQL_CREATE_PGSTATTUPLE': `cat /tmp/bloated_tables.log`"
        continue
    fi

    #echo "$SQL_TABLES"
    TABLES=$(psql -XAtq -c "$SQL_TABLES" "$db")
    if [ $? -ne 0 ]; then
        echo "could not list tables...skipping DB $db"
        continue
    fi

    for table in $TABLES; do
        #echo "checking table $table ..."
        TUPLE_PCT=$(PGOPTIONS="-c statement_timeout=$STATEMENT_TIMEOUT" psql -XAtq -c "select approx_tuple_percent::int from pgstattuple_approx('$table')" "$db")
        if [ $? -eq 0 ]; then
            if [ $TUPLE_PCT -lt $MIN_BLOAT_PCT_TO_REPORT ]; then
                SIZE=$(psql -XAtq -c "select pg_size_pretty(pg_table_size('$table'))" $db)
                echo "found bloated table: $table , live row pct: $TUPLE_PCT %, current table size $SIZE"
                let i++
                if [ $DO_COMPACT -gt 0 ]; then
                    echo "VACUUM FULL ANALYZE $table ..."
                    PGOPTIONS="-c statement_timeout=$VACUUM_FULL_TIMEOUT" psql -XAtq -c "VACUUM FULL ANALYZE $table" $db
                    if [ $? -eq 0 ]; then
                      SIZE=$(psql -XAtq -c "select pg_size_pretty(pg_table_size('$table'))" $db)
                      echo "new size: $SIZE"
                    fi
                fi
            fi
        fi
    done

done

echo ""
echo "Done. Bloated tables found: $i"
