CREATE OR REPLACE FUNCTION public.partitioned_table_total_data_size(text)
 RETURNS numeric
 LANGUAGE sql
AS $function$
select coalesce((select sum(pg_table_size(inhrelid::regclass)) from pg_inherits where inhparent=$1::regclass), 0) + (select pg_table_size($1::regclass));
$function$;

comment on function public.partitioned_table_total_data_size(text) is 'data size (all forks) of parent and all children';

