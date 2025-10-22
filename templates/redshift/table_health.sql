-- Métricas de saúde da tabela
-- {{ schema }}, {{ table }}, {{ ts_col (opcional) }}
WITH rowcount AS (
  SELECT COUNT(*) AS row_count FROM {{ schema }}.{{ table }}
), est_rows AS (
  SELECT SUM(rows) AS est_rows FROM svv_table_info WHERE schema = '{{ schema }}' AND table = '{{ table }}'
), last_ts AS (
  SELECT MAX({{ ts_col }}) AS max_ts FROM {{ schema }}.{{ table }}
)
SELECT
  rowcount.row_count,
  est_rows.est_rows,
  last_ts.max_ts
FROM rowcount, est_rows, last_ts;