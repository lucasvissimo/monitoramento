# Monitor DW - Sistema de Monitoramento de Data Warehouse

Sistema modularizado para monitoramento de queries Redshift, refresh do Power BI, tickets do Jira e KPIs da Evino.

## 📁 Estrutura do Projeto

```
monitor_dw/
├─ app.py                    # Aplicação principal Streamlit
├─ requirements.txt          # Dependências Python
├─ monitor_dw/               # Pacote da aplicação
│  ├─ __init__.py
│  ├─ config.py              # Constantes, timezones, helpers de formatação
│  ├─ db.py                  # Conexões & executores (Redshift/Postgres)
│  ├─ services/
│  │  ├─ __init__.py
│  │  ├─ redshift_monitor.py # Contagem/lista de queries "engasgadas"
│  │  ├─ powerbi.py          # Último refresh backlog_sap (Postgres)
│  │  ├─ jira_client.py      # Consultas Jira (count + issues)
│  │  ├─ kpis.py             # KPIs Evino (today/month/forecast)
│  │  └─ alerts.py           # Slack webhook + montagem de blocks
│  └─ ui/
│     ├─ __init__.py
│     ├─ cards.py            # Funções de render dos 3 cards + KPIs
│     └─ sidebar.py          # Controles de sidebar (thresholds, auto-refresh)
```

## 🚀 Instalação

1. **Instalar dependências:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configurar secrets:**
   Crie um arquivo `.streamlit/secrets.toml` com as configurações:
   ```toml
   [dw_vissimo]
   host = "seu-redshift-host"
   port = 5439
   dbname = "seu-db"
   user = "seu-usuario"
   password = "sua-senha"

   [postgres]
   host = "seu-postgres-host"
   port = 5432
   dbname = "seu-db"
   user = "seu-usuario"
   password = "sua-senha"

   [jira]
   base_url = "https://seu-jira.atlassian.net"
   email = "seu-email@empresa.com"
   api_token = "seu-token"

   [slack]
   webhook_url = "https://hooks.slack.com/services/..."
   ```

3. **Executar aplicação:**
   ```bash
   streamlit run app.py
   ```

## 📊 Funcionalidades

### 🧭 Visão Geral
- Resumo em tempo real de todas as métricas
- Indicadores de status (OK/Warning/Error)
- Diagnóstico do Slack

### 🟥 Redshift
- Monitoramento de queries "engasgadas" (> limite configurável)
- Lista detalhada de queries em execução
- Comandos para cancelar queries

### 🟨 Power BI
- Último refresh do backlog_sap
- Alertas de atraso configuráveis
- Status em tempo real

### 🟦 Jira
- Contagem de tickets abertos do projeto TD
- Lista de tickets com detalhes
- Filtros por status e responsável

### 🍇 KPIs Evino
- Receita do dia/mês
- Forecast vs realizado
- Top seller da última hora
- Margens (CM1/CM2)

### 🆕 Monitoramentos
- Criação de monitores customizados
- Métricas de tabelas Redshift
- Preview de dados
- Histórico de atualizações

### 📊 Histórico
- Log de logins de usuários
- Estatísticas de erros
- Resumos diários
- Exportação de dados

## ⚙️ Configurações

### Thresholds Padrão
- **Redshift:** 10 minutos (queries > limite)
- **Power BI:** 180 minutos (refresh > limite)
- **Auto-refresh:** 60 segundos

### Timezone
- **Padrão:** America/Sao_Paulo
- Configurável em `monitor_dw/config.py`

## 🔧 Desenvolvimento

### Adicionando Novos Serviços
1. Crie um novo arquivo em `monitor_dw/services/`
2. Implemente as funções necessárias
3. Importe no `app.py` principal
4. Adicione na interface conforme necessário

### Adicionando Novos Cards
1. Crie funções de render em `monitor_dw/ui/cards.py`
2. Importe e use no `app.py`
3. Mantenha consistência visual com os cards existentes

### Modificando Configurações
- Edite `monitor_dw/config.py` para constantes globais
- Use `st.secrets` para configurações sensíveis
- Mantenha configurações de UI centralizadas

## 📝 Logs e Monitoramento

- **Histórico de logins:** SQLite local
- **Erros:** Log automático com deduplicação
- **Alertas Slack:** Envio automático com rate limiting
- **Cache:** TTL configurável por tipo de dados

## 🛠️ Troubleshooting

### Problemas Comuns

1. **Erro de conexão Redshift/Postgres:**
   - Verifique credenciais em `.streamlit/secrets.toml`
   - Teste conectividade de rede
   - Verifique permissões de usuário

2. **Slack não envia alertas:**
   - Teste webhook no diagnóstico
   - Verifique se webhook não foi revogado
   - Confirme formato da URL

3. **Jira não carrega tickets:**
   - Verifique API token
   - Confirme permissões do usuário
   - Teste JQL manualmente

### Debug
- Use a aba "Debug" em Monitoramentos
- Verifique logs no console
- Teste conexões individualmente

## 📄 Licença

Projeto interno - Monitor DW Team