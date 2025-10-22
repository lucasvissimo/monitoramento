-- Minutos desde o último refresh da mv_backlog_sap (UTC)
WITH last_refresh AS (
  SELECT DATE_TRUNC('minute', MAX(backlog_sap.etl_load_date AT TIME ZONE 'UTC')) AS last_utc
  FROM robos_bi.mv_backlog_sap AS backlog_sap
)
SELECT EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'UTC' - last_utc)) / 60 AS minutes_since_refresh
FROM last_refresh;