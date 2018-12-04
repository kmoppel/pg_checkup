CREATE OR REPLACE FUNCTION public.partitioned_table_total_size(text)
 RETURNS numeric
 LANGUAGE sql
AS $function$
select coalesce((select sum(pg_total_relation_size(inhrelid::regclass)) from pg_inherits where inhparent=$1::regclass), 0) + (select pg_total_relation_size($1::regclass));
$function$;

comment on function public.partitioned_table_total_size IS 'data (all forks) + indexes of parent table and all children';

