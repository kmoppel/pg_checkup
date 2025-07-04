select
  pg_size_pretty(count(*) * (select setting::int from pg_settings where name = 'wal_segment_size')) as approx_wal_folder_size
from
  pg_ls_dir(current_setting('data_directory')|| '/pg_wal') f
where
  f ~ '^0000'
  and length(f) = 24
;