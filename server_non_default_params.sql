SELECT
  name,
  category,
  current_setting(name),
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
