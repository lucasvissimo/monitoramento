-- Minutos desde o último timestamp em uma coluna
-- {{ schema }}, {{ table }}, {{ ts_col }}
WITH last_ts AS (
  SELECT MAX({{ ts_col }}) AS ts
  FROM {{ schema }}.{{ table }}
)
SELECT EXTRACT(EPOCH FROM ((GETDATE() AT TIME ZONE 'UTC') - (ts AT TIME ZONE 'UTC'))) / 60 AS value
FROM last_ts;