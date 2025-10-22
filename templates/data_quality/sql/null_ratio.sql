-- Percentual de nulos em uma coluna
-- {{ schema }}, {{ table }}, {{ column }}
SELECT (SUM(CASE WHEN {{ column }} IS NULL THEN 1 ELSE 0 END)::decimal / NULLIF(COUNT(*),0)) AS value
FROM {{ schema }}.{{ table }};