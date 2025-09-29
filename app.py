# -*- coding: utf-8 -*-
"""
Monitor DW - Sistema de Monitoramento de Data Warehouse
Aplicação principal modularizada
"""

import streamlit as st
import time
import json
import os
import uuid
import pandas as pd

# Imports dos módulos
from monitor_dw.config import TZ, PRIMARY, OK, WARN, ERR, MUTE

# Importação condicional do db para evitar erro de psycopg2
DB_AVAILABLE = False
init_history_db = lambda: None
log_error = lambda x, y: None

def init_db_if_available():
    """Inicializa o banco de dados se disponível"""
    global DB_AVAILABLE, init_history_db, log_error
    try:
        from monitor_dw.db import init_history_db as _init_history_db, log_error as _log_error
        init_history_db = _init_history_db
        log_error = _log_error
        DB_AVAILABLE = True
        return True
    except ImportError as e:
        st.error(f"⚠️ Módulo de banco de dados não disponível: {e}")
        return False

# Imports condicionais para lidar com dependências ausentes
try:
    from monitor_dw.services.redshift_monitor import get_queries_over_threshold, get_queries_list
    REDSHIFT_AVAILABLE = True
except ImportError as e:
    st.error(f"⚠️ Módulo Redshift não disponível: {e}")
    REDSHIFT_AVAILABLE = False
    get_queries_over_threshold = lambda x: 0
    get_queries_list = lambda x, y: None

try:
    from monitor_dw.services.powerbi import get_last_refresh, has_powerbi_anomaly, get_refresh_status_info
    POWERBI_AVAILABLE = True
except ImportError as e:
    st.error(f"⚠️ Módulo PowerBI não disponível: {e}")
    POWERBI_AVAILABLE = False
    get_last_refresh = lambda: (None, 0)
    has_powerbi_anomaly = lambda x: False
    get_refresh_status_info = lambda x: {"is_anomaly": False}

try:
    from monitor_dw.services.jira_client import get_open_tickets, has_jira_anomaly, format_issues_for_display
    JIRA_AVAILABLE = True
except ImportError as e:
    st.error(f"⚠️ Módulo Jira não disponível: {e}")
    JIRA_AVAILABLE = False
    get_open_tickets = lambda: (0, [])
    has_jira_anomaly = lambda x: False
    format_issues_for_display = lambda x: []

try:
    from monitor_dw.services.kpis import get_all_kpis
    KPIS_AVAILABLE = True
except ImportError as e:
    st.error(f"⚠️ Módulo KPIs não disponível: {e}")
    KPIS_AVAILABLE = False
    get_all_kpis = lambda: {}

try:
    from monitor_dw.services.alerts import send_alert_if_needed
    ALERTS_AVAILABLE = True
except ImportError as e:
    st.error(f"⚠️ Módulo de alertas não disponível: {e}")
    ALERTS_AVAILABLE = False
    send_alert_if_needed = lambda *args, **kwargs: (False, "Alertas desabilitados")

try:
    from monitor_dw.ui.sidebar import render_auth_ui, render_auth_sidebar, render_auto_refresh_controls, render_system_info
    from monitor_dw.ui.cards import (
        render_overview_card, render_redshift_card, render_powerbi_card, 
        render_jira_card, render_kpis_card, render_slack_diagnostic_card
    )
    UI_AVAILABLE = True
except ImportError as e:
    st.error(f"⚠️ Módulos de UI não disponíveis: {e}")
    UI_AVAILABLE = False

# ======================== PAGE CONFIG ========================
st.set_page_config(
    page_title="Monitor DW | Queries & Refresh",
    page_icon="⏱️",
    layout="wide",
    menu_items={
        "About": "Monitoramento de DW — Redshift, Power BI e Jira • Feito com ❤️ em Streamlit"
    },
)

# ======================== THEME & CSS ========================
VARS_CSS = f":root {{ --primary: {PRIMARY}; --ok: {OK}; --warn: {WARN}; --err: {ERR}; --mute: {MUTE}; }}\n"
OTHER_CSS = """
html, body, [class^="css"] { font-size: 15px; }
.app-header {
  display:flex;align-items:center;gap:.75rem;flex-wrap:wrap;
  padding:.5rem 0 1rem 0;border-bottom:1px solid rgba(255,255,255,.08);
}
.badge {
  display:inline-flex;align-items:center;gap:.4rem;
  padding:.2rem .55rem;border-radius:999px;font-weight:600;font-size:.80rem;
  background:#1113;border:1px solid #ffffff15;
}
.badge.ok   { background-color: color-mix(in srgb, var(--ok) 22%, transparent); color: #d9ffe5;border-color: color-mix(in srgb, var(--ok) 55%, #000); }
.badge.warn { background-color: color-mix(in srgb, var(--warn) 22%, transparent); color: #fff4d6;border-color: color-mix(in srgb, var(--warn) 55%, #000); }
.badge.err  { background-color: color-mix(in srgb, var(--err) 22%, transparent);  color: #ffe0e0;border-color: color-mix(in srgb, var(--err) 55%, #000); }
.badge.mute { background-color: #2c2f36; color: #d1d5db; border-color:#3b3f46; }
.card {
  background: linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,.01));
  border: 1px solid rgba(255,255,255,.08);
  border-radius: 16px; padding: 18px; transition: .2s ease; backdrop-filter: blur(6px);
}
.card:hover { border-color: rgba(255,255,255,.16); box-shadow: 0 6px 24px rgba(0,0,0,.18); }
.card h3 { margin: 0 0 .35rem 0; font-size: 1.05rem; }
.metric-row { display:grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 16px; }
.metric { border-radius: 14px; padding: 14px; border:1px solid rgba(255,255,255,.12); }
.metric .label { color: #9ca3af; font-size:.8rem; }
.metric .value { font-size: 1.6rem; font-weight: 800; line-height: 1.2; }
.metric .delta { font-size:.85rem; opacity:.85; }
.stDataFrame { border-radius: 12px; overflow:hidden; }
hr { border: none; height: 1px; background: linear-gradient(90deg, transparent, #ffffff22, transparent); margin: .75rem 0; }
.footer { color:#9ca3af; font-size:.8rem; text-align:right; padding-top:.5rem; }

/* auth cards */
.auth-card { background: #1116; border:1px solid #ffffff22; border-radius:14px; padding:18px; }
.auth-card h3 { margin:.2rem 0 1rem 0 }

/* nicer tabs */
div[role="tablist"] > button[role="tab"] {
  border-radius: 999px; margin-right: 6px; padding: 6px 12px;
  border: 1px solid #ffffff10; background: #1116;
}
div[role="tablist"] > button[aria-selected="true"] {
  background: color-mix(in srgb, var(--primary) 18%, transparent);
  border-color: color-mix(in srgb, var(--primary) 50%, #000);
}

/* primary buttons */
button[kind="primary"] {
  background: var(--primary) !important; color: #001018 !important; font-weight: 700;
}
button[kind="primary"]:hover { filter: brightness(1.08); }

/* inputs subtle border */
label + div input, label + div textarea, .stSelectbox > div > div {
  border-radius: 10px !important; border: 1px solid #ffffff22 !important;
}

/* dataframe polish */
div[data-testid="stDataFrame"] table thead tr th { position: sticky; top: 0; backdrop-filter: blur(4px); }
div[data-testid="stDataFrame"] tbody tr:nth-child(odd) { background: rgba(255,255,255,0.02); }

/* responsive metrics */
@media (max-width: 1100px) { .metric-row { grid-template-columns: repeat(2, minmax(0,1fr)); } }
@media (max-width: 700px)  { .metric-row { grid-template-columns: 1fr; } }

/* status indicators */
.status-indicator {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.25rem 0.75rem;
  border-radius: 999px;
  font-size: 0.875rem;
  font-weight: 600;
}
.status-indicator.ok { background: rgba(34, 197, 94, 0.1); color: #22c55e; border: 1px solid rgba(34, 197, 94, 0.3); }
.status-indicator.warn { background: rgba(245, 158, 11, 0.1); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.3); }
.status-indicator.error { background: rgba(239, 68, 68, 0.1); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }

/* filter controls */
.filter-controls {
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 12px;
  padding: 1rem;
  margin-bottom: 1rem;
}

/* performance indicators */
.perf-indicator {
  font-size: 0.75rem;
  color: #9ca3af;
  text-align: right;
  margin-top: 0.5rem;
}
"""
CUSTOM_CSS = "<style>" + VARS_CSS + OTHER_CSS + "</style>"
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ======================== INITIALIZATION ========================
# Initialize database
# Inicializar banco de dados se disponível
init_db_if_available()

# ======================== HEADER ========================
st.markdown(
    f"""
    <div class="app-header">
      <h1 style="margin:0">📊 Monitor DW — Queries & Refresh</h1>
      <span class="badge mute">Timezone: America/Sao_Paulo</span>
    </div>
    """,
    unsafe_allow_html=True,
)
st.caption("Redshift: queries > limite | Postgres: último refresh backlog_sap | Jira | KPIs Evino")

# ======================== AUTHENTICATION ========================
render_auth_ui()

# ======================== SIDEBAR ========================
render_auth_sidebar()
redshift_threshold, refresh_alert_min, auto_refresh, auto_refresh_sec = render_auto_refresh_controls()
render_system_info()

# ======================== MAIN CONTENT ========================
# Tabs
tab_overview, tab_redshift, tab_powerbi, tab_jira, tab_kpis, tab_kestra, tab_monitors, tab_history = st.tabs([
    "🧭 Visão Geral", "🟥 Redshift", "🟨 Power BI (CD)", "🟦 Jira", "🍇 KPIs Evino", "🔄 Kestra", "🆕 Monitoramentos", "📊 Histórico"
])

# Funções de verificação de anomalias
def has_query_anomaly(running_over: int, threshold: int) -> bool:
    return (running_over is not None) and (running_over >= 1) and (threshold >= 10)

def has_kpi_anomaly(kpi_evino_pct, kpi_min: float) -> bool:
    if kpi_evino_pct is None:
        return False
    return float(kpi_evino_pct) < float(kpi_min)

# -------- VISÃO GERAL --------
with tab_overview:
    # Coleta dados para overview
    running_over = get_queries_over_threshold(redshift_threshold)
    last_refresh_utc, age_min = get_last_refresh()
    powerbi_bad = has_powerbi_anomaly(last_refresh_utc)
    total_abertos, issues = get_open_tickets()
    kpis_data = get_all_kpis()

    # Monitors quick
    def _load_monitors_quick() -> list[dict]:
        try:
            with open(".monitors.json", "r", encoding="utf-8") as f:
                return json.load(f).get("monitors", [])
        except Exception:
            return []
    
    mons = _load_monitors_quick()
    stale = 0  # Placeholder - could be enhanced later

    # Calcular status das anomalias
    from monitor_dw.config import KPI_ALERT_PCT
    kpi_evino_pct = None  # (não calculado no script original)
    
    query_bad   = has_query_anomaly(running_over, redshift_threshold)
    jira_bad    = has_jira_anomaly(total_abertos)
    kpi_bad     = has_kpi_anomaly(kpi_evino_pct, KPI_ALERT_PCT)
    any_bad     = query_bad or powerbi_bad or jira_bad or kpi_bad

    # Render overview card
    render_overview_card(
        running_over, last_refresh_utc, powerbi_bad, total_abertos, 
        kpis_data["today"]["today_revenue"], kpis_data["fc_day"]["today_forecast"],
        redshift_threshold, len(mons), stale
    )

    # Diagnóstico Slack
    render_slack_diagnostic_card()

    # Status das anomalias
    st.write({
        "webhook_configurado": bool(st.secrets.get("slack", {}).get("webhook_url", "") and "hooks.slack.com" in st.secrets.get("slack", {}).get("webhook_url", "")),
        "query_bad": query_bad,
        "powerbi_bad": powerbi_bad,
        "jira_bad": jira_bad,
        "kpi_bad": kpi_bad,
        "any_bad": any_bad,
    })

# -------- REDSHIFT --------
with tab_redshift:
    running_over = get_queries_over_threshold(redshift_threshold)
    df_list = None
    if running_over > 0:
        df_list = get_queries_list(redshift_threshold, 20)
        log_error("redshift_queries_over_10min", f"Count: {running_over}, Threshold: {redshift_threshold}min")
    
    render_redshift_card(running_over, redshift_threshold, df_list)

# -------- POWER BI --------
with tab_powerbi:
    refresh_info = get_refresh_status_info(get_last_refresh()[0])
    if refresh_info["is_anomaly"]:
        log_error("powerbi_refresh_delay", f"Last refresh: {refresh_info['last_refresh_utc']}, Current: {time.time()}")
    
    render_powerbi_card(refresh_info, refresh_alert_min)

# -------- JIRA --------
with tab_jira:
    try:
        st.caption("🔍 Carregando dados do Jira...")
        total_abertos, issues = get_open_tickets()
        
        # Debug info
        st.caption(f"📊 Debug: total_abertos={total_abertos}, issues_count={len(issues) if issues else 0}")
        
        # Registrar chamados abertos no histórico
        if total_abertos > 0 and DB_AVAILABLE:
            from monitor_dw.db import log_jira_tickets
            log_jira_tickets(total_abertos)
        
        if issues:
            formatted_issues = format_issues_for_display(issues)
            render_jira_card(total_abertos, formatted_issues)
        else:
            st.warning("⚠️ Nenhum chamado encontrado ou erro na consulta")
            st.caption("Verifique se há tickets no projeto TD com status 'To Do' ou 'In Progress'")
            
    except Exception as e:
        st.error(f"❌ Erro ao carregar dados do Jira: {str(e)}")
        st.caption("Verifique as configurações do Jira no arquivo secrets.toml")

# -------- KPIs EVINO --------
with tab_kpis:
    kpis_data = get_all_kpis()
    render_kpis_card(kpis_data)

# -------- KESTRA --------
with tab_kestra:
    from monitor_dw.ui.cards import render_kestra_card
    # Lista de flows específicos para monitorar (opcional)
    # Se deixar vazio, tentará obter automaticamente
    kestra_flows = []  # Exemplo: ["flow1", "flow2", "flow3"]
    render_kestra_card(kestra_flows)

# -------- MONITORAMENTOS (NOVO) --------
with tab_monitors:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("🆕 Criar/visualizar monitoramentos do DW")

    MON_PATH = ".monitors.json"

    @st.cache_data(show_spinner=False)
    def _load_monitors() -> dict:
        if not os.path.exists(MON_PATH):
            return {"monitors": []}
        with open(MON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_monitors(data: dict) -> None:
        with open(MON_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # Import functions from redshift_monitor
    from monitor_dw.services.redshift_monitor import (
        get_schemas, get_tables, get_columns, get_table_metrics, get_table_preview
    )

    # UI: seleção
    st.markdown("### 📋 Selecionar tabela para monitorar")
    
    # Schema selection
    st.markdown("**1. Escolha o Schema:**")
    try:
        schemas = get_schemas()
        if not schemas:
            st.error("❌ Não foi possível carregar os schemas. Verifique a conexão com Redshift.")
            st.stop()
        schema = st.selectbox("Schema", options=schemas, index=schemas.index("public") if "public" in schemas else 0, key="mon_schema")
    except Exception as e:
        st.error(f"❌ Erro ao carregar schemas: {str(e)}")
        st.stop()
    
    # Table selection
    st.markdown("**2. Escolha a Tabela:**")
    if schema:
        try:
            tables = get_tables(schema)
            if tables:
                st.success(f"✅ Encontradas {len(tables)} tabelas no schema '{schema}'")
                # Show first few table names as preview
                preview = ", ".join(tables[:3])
                if len(tables) > 3:
                    preview += f" (+{len(tables)-3} mais)"
                st.caption(f"📋 Exemplos: {preview}")
            else:
                st.warning("⚠️ Nenhuma tabela encontrada")
                st.caption("Verifique permissões ou se o schema existe")
        except Exception as e:
            st.error(f"❌ Erro ao listar tabelas: {e}")
            tables = []
    else:
        tables = []
        
    if tables:
        table = st.selectbox("Tabela", options=tables, key=f"mon_table_{schema}")
    else:
        table = st.selectbox("Tabela", options=["(nenhuma)"] , index=0, key=f"mon_table_empty_{schema}")
        table = None
    
    # Timestamp column selection
    st.markdown("**3. Coluna de Data/Hora (opcional):**")
    df_cols = get_columns(schema, table) if (schema and table) else pd.DataFrame(columns=["column","type"])
    # Sugerir colunas de timestamp
    ts_suggestions = [c for c,t in df_cols.values if ("timestamp" in str(t).lower() or "date" in str(t).lower() or c.lower().endswith(("_at","_dt","_date")))]
    ts_col = st.selectbox("Coluna de data/hora (opcional)", options=[""] + ts_suggestions + df_cols["column"].tolist() if not df_cols.empty else [""], index=0, help="Usada para achar última atualização e ordenar prévia")
    ts_col = ts_col or None

    st.divider()
    if schema and table:
        # Métricas da tabela
        st.markdown("### 📊 Métricas da Tabela")
        metrics = get_table_metrics(schema, table, ts_col)
        m1, m2, m3 = st.columns(3)
        with m1:
            st.markdown(f"<div class='metric'><div class='label'>Linhas (COUNT)</div><div class='value'>{metrics.get('row_count') or '—'}</div></div>", unsafe_allow_html=True)
        with m2:
            st.markdown(f"<div class='metric'><div class='label'>Linhas (svv_table_info)</div><div class='value'>{metrics.get('est_rows') or '—'}</div></div>", unsafe_allow_html=True)
        with m3:
            last_ts = metrics.get("max_ts")
            if isinstance(last_ts, pd.Timestamp):
                last_ts_s = last_ts.tz_convert(TZ).strftime("%d/%m %H:%M")
            else:
                last_ts_s = "—"
            st.markdown(f"<div class='metric'><div class='label'>Última atualização</div><div class='value'>{last_ts_s}</div></div>", unsafe_allow_html=True)

        # Prévia
        with st.expander("Prévia (20 mais recentes)", expanded=False):
            try:
                df_prev = get_table_preview(schema, table, ts_col, 20)
                st.dataframe(df_prev, use_container_width=True, height=340)
            except Exception as e:
                st.caption(f"Falha ao consultar prévia: {e}")

        st.divider()
        st.markdown("### 💾 Salvar Monitor")
        mon_name = st.text_input("Nome do monitor", value=f"{schema}.{table}", help="Nome personalizado para identificar este monitor")
        notes = st.text_area("Notas (opcional)", value="", help="Descrição ou observações sobre este monitor")
        
        c1, c2 = st.columns([1,1])
        with c1:
            if st.button("💾 Salvar monitor", type="primary"):
                data = _load_monitors()
                entry = {
                    "id": str(uuid.uuid4()),
                    "schema": schema,
                    "table": table,
                    "ts_col": ts_col,
                    "name": mon_name,
                    "notes": notes,
                    "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                # Evitar duplicados por schema+table
                exists = [m for m in data.get("monitors", []) if m.get("schema") == schema and m.get("table") == table]
                if exists:
                    # Atualiza
                    for m in data["monitors"]:
                        if m.get("schema") == schema and m.get("table") == table:
                            m.update(entry)
                    _save_monitors(data)
                    st.success("✅ Monitor atualizado com sucesso!")
                else:
                    data.setdefault("monitors", []).append(entry)
                    _save_monitors(data)
                    st.success("✅ Monitor salvo com sucesso!")
        with c2:
            if st.button("🔄 Recarregar monitores"):
                _load_monitors.clear()
                st.rerun()

        st.divider()

    # Lista de monitores salvos
    st.markdown("### 📋 Monitores Salvos")
    data = _load_monitors()
    mons = data.get("monitors", [])
    if not mons:
        st.info("ℹ️ Nenhum monitor salvo ainda. Selecione uma tabela acima para criar seu primeiro monitor.")
    else:
        st.caption(f"📊 {len(mons)} monitor(es) configurado(s)")
        
        # Status rápido de cada monitor
        rows = []
        for m in mons:
            try:
                mt = get_table_metrics(m.get("schema"), m.get("table"), m.get("ts_col"))
                max_ts = mt.get("max_ts")
                if isinstance(max_ts, pd.Timestamp):
                    last_ts_s = max_ts.tz_convert(TZ).strftime("%d/%m %H:%M")
                elif m.get("ts_col"):
                    last_ts_s = "Sem dados"
                else:
                    last_ts_s = "Sem coluna de data"
                rows.append({
                    "Nome": m.get("name") or f"{m.get('schema')}.{m.get('table')}",
                    "Schema": m.get("schema"),
                    "Tabela": m.get("table"),
                    "Última atualização": last_ts_s,
                    "Linhas": mt.get("row_count"),
                    "Coluna TS": m.get("ts_col") or "—",
                    "Notas": (m.get("notes") or "")[:80],
                })
            except Exception as e:
                rows.append({
                    "Nome": m.get("name"),
                    "Schema": m.get("schema"),
                    "Tabela": m.get("table"),
                    "Última atualização": "❌ erro",
                    "Linhas": "❌ erro",
                    "Coluna TS": m.get("ts_col") or "—",
                    "Notas": str(e)[:80],
                })
        dfm = pd.DataFrame(rows)
        st.dataframe(dfm, use_container_width=True, height=360, hide_index=True)
        
        # Adicionar seção de debug no final
        with st.expander("🔧 Debug - Testar Conexões", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Testar Conexão Redshift"):
                    try:
                        if DB_AVAILABLE:
                            from monitor_dw.db import get_redshift_conn
                            with get_redshift_conn() as conn:
                                with conn.cursor() as cur:
                                    cur.execute("SELECT 1")
                                    st.success("✅ Conexão Redshift OK")
                        else:
                            st.warning("⚠️ Módulo de banco de dados não disponível")
                    except Exception as e:
                        st.error(f"❌ Erro de conexão: {e}")
            
            with col2:
                if st.button("Listar Todos os Schemas"):
                    try:
                        schemas = get_schemas()
                        st.write(f"📋 Schemas encontrados ({len(schemas)}):")
                        st.write(schemas)
                    except Exception as e:
                        st.error(f"❌ Erro: {e}")

    st.markdown('</div>', unsafe_allow_html=True)

# -------- HISTÓRICO --------
with tab_history:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("📊 Histórico de Logins e Erros")

    # Filtros de período
    st.markdown("### 📅 Filtros")
    col_filter1, col_filter2, col_filter3 = st.columns(3)
    with col_filter1:
        days_filter = st.selectbox("Período", options=[1, 3, 7, 14, 30], index=2, help="Selecione o número de dias para análise")
    with col_filter2:
        if st.button("📥 Exportar dados", help="Exporta os dados do histórico"):
            if DB_AVAILABLE:
                from monitor_dw.db import get_error_stats
                stats = get_error_stats(days_filter)
            else:
                st.warning("⚠️ Módulo de banco de dados não disponível")
                stats = {"error_counts": {}}
            if stats["error_counts"]:
                df_export = pd.DataFrame(list(stats["error_counts"].items()), columns=["Tipo de Erro", "Quantidade"])
                csv = df_export.to_csv(index=False)
                st.download_button("💾 Download CSV", csv, "historico_erros.csv", "text/csv")
            else:
                st.warning("Nenhum dado para exportar")
    with col_filter3:
        if st.button("🧹 Limpar dados duplicados", help="Remove entradas duplicadas de erros"):
            if DB_AVAILABLE:
                from monitor_dw.db import cleanup_duplicate_errors
                cleanup_duplicate_errors()
                st.success("✅ Dados duplicados removidos!")
                st.rerun()
            else:
                st.warning("⚠️ Módulo de banco de dados não disponível")
    
    # Botão para limpar conexões de banco
    if st.button("🔄 Limpar Conexões DB", help="Limpa todas as conexões de banco de dados em cache"):
        if DB_AVAILABLE:
            from monitor_dw.db import clear_all_db_connections
            if clear_all_db_connections():
                st.success("✅ Conexões de banco limpas!")
            else:
                st.error("❌ Erro ao limpar conexões")
            st.rerun()
        else:
            st.warning("⚠️ Módulo de banco de dados não disponível")
    
    # Get error statistics
    if DB_AVAILABLE:
        from monitor_dw.db import get_error_stats
        stats = get_error_stats(days_filter)
    else:
        stats = {"error_counts": {}, "daily_summaries": []}
    
    # ======================== GRÁFICOS ========================
    st.markdown("### 📈 Análise de Tendências")
    
    # Preparar dados para gráficos
    daily_summaries = stats["daily_summaries"]
    if daily_summaries:
        df_daily = pd.DataFrame(daily_summaries, columns=[
            "Data", "Queries >10min", "Chamados Jira", "Delays PowerBI", "Anomalias KPI"
        ])
        
        # Converter data para datetime
        df_daily["Data"] = pd.to_datetime(df_daily["Data"])
        df_daily = df_daily.sort_values("Data")
        
        # Criar gráficos
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            st.markdown("#### 🔴 Queries > 10min por Dia")
            if not df_daily["Queries >10min"].isna().all():
                st.line_chart(
                    df_daily.set_index("Data")["Queries >10min"],
                    use_container_width=True,
                    height=300
                )
            else:
                st.info("Nenhum dado de queries disponível")
        
        with col_chart2:
            st.markdown("#### 🟦 Chamados Jira por Dia")
            if not df_daily["Chamados Jira"].isna().all():
                st.bar_chart(
                    df_daily.set_index("Data")["Chamados Jira"],
                    use_container_width=True,
                    height=300
                )
            else:
                st.info("Nenhum dado de chamados disponível")
        
        col_chart3, col_chart4 = st.columns(2)
        
        with col_chart3:
            st.markdown("#### 🟨 Delays PowerBI por Dia")
            if not df_daily["Delays PowerBI"].isna().all():
                st.area_chart(
                    df_daily.set_index("Data")["Delays PowerBI"],
                    use_container_width=True,
                    height=300
                )
            else:
                st.info("Nenhum delay do PowerBI registrado")
        
        with col_chart4:
            st.markdown("#### 🍇 Anomalias KPI por Dia")
            if not df_daily["Anomalias KPI"].isna().all():
                st.line_chart(
                    df_daily.set_index("Data")["Anomalias KPI"],
                    use_container_width=True,
                    height=300
                )
            else:
                st.info("Nenhuma anomalia de KPI registrada")
        
        st.divider()
        
        # Resumo estatístico
        st.markdown("### 📊 Resumo Estatístico")
        col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
        
        with col_stat1:
            total_queries = df_daily["Queries >10min"].sum()
            st.metric("Total Queries >10min", total_queries, help="Soma de todas as queries que rodaram mais de 10 minutos")
        
        with col_stat2:
            total_jira = df_daily["Chamados Jira"].sum()
            st.metric("Total Chamados Jira", total_jira, help="Soma de todos os chamados abertos")
        
        with col_stat3:
            total_powerbi = df_daily["Delays PowerBI"].sum()
            st.metric("Total Delays PowerBI", total_powerbi, help="Soma de todos os delays do PowerBI")
        
        with col_stat4:
            total_kpi = df_daily["Anomalias KPI"].sum()
            st.metric("Total Anomalias KPI", total_kpi, help="Soma de todas as anomalias de KPI")
    
    else:
        st.info("Nenhum resumo diário disponível para gerar gráficos.")
    
    st.divider()
    
    # ======================== TABELAS DETALHADAS ========================
    st.markdown("### 📋 Dados Detalhados")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"#### 🔐 Logins dos últimos {days_filter} dias")
        import sqlite3
        from monitor_dw.config import HISTORY_DB_PATH
        conn = sqlite3.connect(HISTORY_DB_PATH)
        conn.execute("PRAGMA encoding = 'UTF-8'")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT username, COUNT(*) as login_count, MAX(login_time) as last_login
            FROM user_logins 
            WHERE login_time >= datetime('now', '-{} days')
            GROUP BY username
            ORDER BY login_count DESC
        """.format(days_filter))
        login_stats = cursor.fetchall()
        conn.close()
        
        if login_stats:
            df_logins = pd.DataFrame(login_stats, columns=["Usuário", "Logins", "Último Login"])
            # Convert to datetime and format with timezone awareness
            df_logins["Último Login"] = pd.to_datetime(df_logins["Último Login"], utc=True).dt.tz_convert(TZ).dt.strftime("%d/%m %H:%M")
            st.dataframe(df_logins, use_container_width=True, hide_index=True, height=200)
        else:
            st.caption("Nenhum login registrado nos últimos 7 dias.")
    
    with col2:
        st.markdown(f"#### ⚠️ Erros dos últimos {days_filter} dias")
        
        error_counts = stats["error_counts"]
        if error_counts:
            df_errors = pd.DataFrame(list(error_counts.items()), columns=["Tipo de Erro", "Quantidade"])
            st.dataframe(df_errors, use_container_width=True, hide_index=True, height=200)
        else:
            st.caption("Nenhum erro registrado nos últimos 7 dias.")
    
    # Tabela de resumo diário
    if daily_summaries:
        st.markdown("#### 📈 Resumo Diário Detalhado")
        st.dataframe(df_daily, use_container_width=True, hide_index=True, height=300)
    
    # Manual update button
    if st.button("🔄 Atualizar Histórico", help="Recarrega todos os dados do histórico"):
        st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)

# ======================== SLACK ALERTS ========================
# Coleta de variáveis dos painéis já renderizados (para não mudar lógica)
running_over = get_queries_over_threshold(redshift_threshold)
last_refresh_utc, age_min = get_last_refresh()
total_abertos, _ = get_open_tickets()

# Registrar chamados do Jira no histórico (backup)
if total_abertos > 0 and DB_AVAILABLE:
    from monitor_dw.db import log_jira_tickets
    log_jira_tickets(total_abertos)

# Recalcular anomalias
query_bad = has_query_anomaly(running_over, redshift_threshold)
powerbi_bad = has_powerbi_anomaly(last_refresh_utc)
jira_bad = has_jira_anomaly(total_abertos)

# Do not send if disabled due to invalid webhook
if st.session_state.get("disable_slack_alerts"):
    st.caption("🔕 Slack alerts desativados devido a webhook inválido. Faça um teste com um webhook válido para reativar.")
else:
    sent, message = send_alert_if_needed(
        query_bad, powerbi_bad, jira_bad, running_over, 
        last_refresh_utc, total_abertos, redshift_threshold, age_min
    )
    st.caption(message)

# ======================== RODAPÉ ========================
st.markdown(
    """
    <div class="footer">
      <span>Monitor DW VISSIMO</span>
    </div>
    """,
    unsafe_allow_html=True,
)