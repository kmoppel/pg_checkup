select
  count(*)*16 size_mb
from
  pg_ls_dir(current_setting('data_directory')|| '/pg_xlog') f
where
  f like '00000%'
  and length(f) = 24
