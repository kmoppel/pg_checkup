SELECT
  name,
  -- category,
  case when length(current_setting(name)) < 50 then current_setting(name) else current_setting(name)::varchar(47) || '...' end as current_setting,
  reset_val,
  boot_val,
  unit,
  source
FROM
  pg_settings
WHERE NOT category ~* 'formatting'
AND NOT category ~* 'logging'
AND NOT category ~* 'ssl'
AND NOT name ~ 'file'
AND NOT name ~ 'directory'
AND NOT name IN ('cluster_name', 'max_stack_depth')
AND boot_val IS DISTINCT FROM reset_val
ORDER BY
  name;
