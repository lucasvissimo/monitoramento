### Runbook de Incidente - Monitor DW

- **Escopo**: Falhas de refresh do Power BI, queries longas no Redshift, chamados Jira abertos
- **Severidades**:
  - **Crítica**: Sem refresh > 6h, >5 queries > 10min, incidentes P1 no Jira
  - **Alerta**: Sem refresh > 3h, >=1 query > 10min, qualquer chamado aberto

#### 1) Power BI atrasado
1. Verificar última atualização (UTC) no painel e no banco `robos_bi.mv_backlog_sap`.
2. Checar erros recentes do pipeline (Kestra/ETL).
3. Reexecutar o fluxo no Kestra se necessário.
4. Comunicar status no Slack (canal #data-ops).

Checklist de mitigação:
- Confirmar credenciais do Postgres
- Validar fonte SAP/landing
- Agendar execução manual caso haja janela de manutenção

#### 2) Redshift com queries longas
1. Consultar `stv_recents` e identificar `pid` e `user_name`.
2. Avaliar impacto e, se necessário, cancelar com `CANCEL <pid>;`.
3. Notificar o responsável e abrir ticket se recorrente.

Checklist de mitigação:
- Confirmar índices/sort keys e plano de execução
- Verificar concorrência e workload management (WLM)

#### 3) Incidentes Jira abertos
1. Validar escopo dos chamados abertos (projeto TD).
2. Priorizar de acordo com severidade/impacto.
3. Garantir dono e próximo passo definidos.

Comunicação padrão:
- Atualizar canal Slack com ETA e responsável.
- Registrar post-mortem se crítico.
