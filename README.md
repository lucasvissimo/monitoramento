# Monitor DW - Queries & Refresh

Sistema de monitoramento para Data Warehouse com Streamlit.

## 🚀 Configuração

### 1. Instalar dependências
```bash
pip install streamlit pandas numpy psycopg2-binary requests
```

### 2. Configurar credenciais
1. Copie o arquivo de exemplo:
   ```bash
   cp .streamlit/secrets.toml.example .streamlit/secrets.toml
   ```

2. Edite `.streamlit/secrets.toml` com suas credenciais:
   - **Redshift**: Host, porta, database, usuário e senha
   - **Postgres**: Host, porta, database, usuário e senha  
   - **Jira**: URL base, email e token da API
   - **Slack**: URL do webhook

### 3. Executar
```bash
streamlit run app.py
```

## 📊 Funcionalidades

- **🧭 Visão Geral**: Resumo em tempo real de todos os sistemas
- **🟥 Redshift**: Monitoramento de queries longas
- **🟨 Power BI**: Status do último refresh
- **🟦 Jira**: Chamados abertos
- **🍇 KPIs Evino**: Métricas de receita e performance
- **🆕 Monitoramentos**: Criação de novos monitores para tabelas
- **📊 Histórico**: Log de logins e erros

## 🔒 Segurança

- Credenciais são armazenadas em `.streamlit/secrets.toml` (não versionado)
- Histórico de logins e erros em SQLite local
- Configurações de monitores em `.monitors.json` (não versionado)

## 📝 Notas

- O arquivo `secrets.toml` não deve ser commitado no Git
- Use variáveis de ambiente em produção
- O banco SQLite `monitor_history.db` é criado automaticamente
