SELECT
  name,
  category,
  case when length(current_setting(name)) < 40 then current_setting(name) else current_setting(name)::varchar(37) || '...' end,
  boot_val,
  unit,
  source
FROM
  pg_settings
WHERE source NOT IN ('default', 'override') --compiled or cmdline
AND NOT category ~* 'client'
AND NOT category ~* 'logging'
AND NOT category ~* 'authent'
ORDER BY
  category, name;
