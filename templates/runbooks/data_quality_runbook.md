### Runbook de Data Quality - Monitor DW

Objetivo: Padronizar investigação e resolução de quebras de qualidade de dados.

Tipos de checagens (templates):
- Rowcount > 0 (tabela não vazia)
- Percentual de nulos por coluna
- Freshness por coluna timestamp

Procedimento:
1. Identificar a checagem quebrada e tabela/coluna afetada.
2. Validar se houve mudanças recentes de schema, upstream ou ETL.
3. Executar query de diagnóstico:
   - Rowcount atual vs histórico
   - Distribuição de nulos por partições/regras de negócio
   - Último timestamp carregado
4. Corrigir causa raiz (fonte, transformação, carga).
5. Reexecutar fluxos no Kestra quando apropriado.
6. Documentar no ticket e atualizar SLAs/SLOs se necessário.

Critérios de severidade:
- Crítico: Freshness > 6h em tabelas core; Rowcount = 0 em DWH core
- Alerta: Freshness > 3h; Null ratio acima do limiar acordado

Boas práticas:
- Introduzir constraints/validações no ETL
- Alertar por Slack com contexto (query, tabela, ts_col)
- Automatizar retentativas com backoff
