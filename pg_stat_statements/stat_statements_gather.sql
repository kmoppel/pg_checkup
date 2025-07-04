create unlogged table if not exists ss_snaps as
	select
		now(),
		userid,
		queryid,
		ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g'))::varchar(500) as query,
		total_time,
		calls,
		mean_time,
		stddev_time,
		shared_blks_hit,
		shared_blks_read,
		temp_blks_read,
		temp_blks_written,
		blk_read_time,
		blk_write_time
	from pg_stat_statements
	where false;

insert into ss_snaps
	select
		now(),
		userid,
		queryid,
		ltrim(regexp_replace(query, E'[ \\t\\n\\r]+' , ' ', 'g'))::varchar(500) as query,
		total_time,
		calls,
		mean_time,
		stddev_time,
		shared_blks_hit,
		shared_blks_read,
		temp_blks_read,
		temp_blks_written,
		blk_read_time,
		blk_write_time
	from pg_stat_statements
	where dbid = (select oid from pg_database where datname = current_database());

\watch 1
