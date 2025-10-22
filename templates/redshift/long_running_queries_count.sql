-- Conta queries em execução acima de um threshold (minutos)
-- Variável: {{ threshold_min }}
SELECT COUNT(*) AS running_over
FROM stv_recents
WHERE status = 'Running'
  AND duration > ({{ threshold_min }} * 60000000);