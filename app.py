from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo
import time
import json
import os
import pandas as pd
import numpy as np
import psycopg2
import streamlit as st
import requests

st.set_page_config(page_title="Monitor DW | Queries & Refresh", page_icon="⏱️", layout="wide")

# ======================== CONFIG FIXA ========================
TZ = ZoneInfo("America/Sao_Paulo")
REDSHIFT_THRESHOLD_MIN = 10     # queries > 10 min
REFRESH_ALERT_MIN = 180         # refresh > 180 min
AUTO_REFRESH_SEC = 60

# ======================== CONEXÕES DB ========================
@st.cache_resource(show_spinner=False)
def get_redshift_conn():
    s = st.secrets["dw_vissimo"]
    conn = psycopg2.connect(
        host=s["host"],
        port=s.get("port", 5439),
        dbname=s["dbname"],
        user=s["user"],
        password=s["password"],
        connect_timeout=8,
        application_name="MonitorDW"
    )
    conn.autocommit = True
    try:
        conn.set_client_encoding("UTF8")
    except Exception:
        pass
    return conn

@st.cache_resource(show_spinner=False)
def get_postgres_conn():
    s = st.secrets["postgres"]
    conn = psycopg2.connect(
        host=s["host"],
        port=s.get("port", 5432),
        dbname=s["dbname"],
        user=s["user"],
        password=s["password"],
        connect_timeout=8,
        application_name="MonitorDW"
    )
    conn.autocommit = True
    try:
        conn.set_client_encoding("UTF8")
    except Exception:
        pass
    return conn

# ======================== EXECUTORES DE QUERY ========================
@st.cache_data(ttl=10, show_spinner=False)
def run_redshift(sql: str) -> pd.DataFrame:
    conn = get_redshift_conn()
    with conn.cursor() as cur:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

@st.cache_data(ttl=10, show_spinner=False)
def run_postgres(sql: str) -> pd.DataFrame:
    conn = get_postgres_conn()
    with conn.cursor() as cur:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

# ======================== UI HEADER ========================
st.title("📊 Monitoramento QUERIES, Atualização CD & Chamados do Jira")
st.caption("Redshift: queries > limite | Postgres: último refresh backlog_sap")

# ======================== SIDEBAR ========================
st.sidebar.header("⚙️ Configurações")
redshift_threshold = st.sidebar.number_input(
    "Limite de execução (min)", min_value=1, max_value=240, value=REDSHIFT_THRESHOLD_MIN, step=1
)
refresh_alert_min = st.sidebar.number_input(
    "Alertar se refresh > (min)", min_value=10, max_value=1440, value=REFRESH_ALERT_MIN, step=10
)
auto_refresh = st.sidebar.checkbox("Autoatualizar página", value=True)
auto_refresh_sec = st.sidebar.number_input(
    "Intervalo de autoatualização (s)", min_value=10, max_value=600, value=AUTO_REFRESH_SEC, step=10
)

# Auto-refresh
if auto_refresh:
    st_autorefresh = getattr(st, "autorefresh", None)
    try:
        if st_autorefresh:
            st_autorefresh(interval=int(auto_refresh_sec) * 1000, key="auto_rf")
        else:
            st.sidebar.info("Sua versão do Streamlit não tem auto-refresh nativo. Use o botão 'Atualizar'.")
    except Exception:
        pass

col1, col2, col3 = st.columns(3)

# ======================== CARTÃO 1 — Redshift ========================
with col1:
    st.subheader("Redshift — Queries Engasgadas")

    TOP_N = 20 

    try:
        with st.spinner("Conectando ao Redshift..."):
            _ = get_redshift_conn()
        st.caption("✅ Conexão Redshift OK")

        # Contagem rápida
        sql_count = f"""
            SELECT COUNT(*) AS running_over
            FROM stv_recents
            WHERE status = 'Running'
              AND duration > {int(redshift_threshold) * 60000000}
        """
        with st.spinner("Buscando contagem de queries..."):
            df_count = run_redshift(sql_count)
        st.caption("✅ Contagem lida")

        running_over = int(df_count.iloc[0]["running_over"]) if not df_count.empty else 0

        st.metric(
            label="Queries rodando demais",
            value=running_over,
            delta=(f"> {int(redshift_threshold)} min" if running_over else "OK"),
        )
        if running_over > 0:
            st.error("⚠️ Existem queries em execução acima do limite configurado.")
        else:
            st.success("✅ Nenhuma query acima do limite no momento.")

        # Lista (sem alertas)
        if running_over > 0:
            sql_list = f"""
                SELECT
                    (r.duration / 60000000.0) AS duration_minutes,
                    'CANCEL ' || r.pid::varchar || ';' AS kill_query,
                    r.pid,
                    r.user_name,
                    r.starttime,
                    r.query
                FROM stv_recents r
                WHERE r.status = 'Running'
                  AND r.duration > {int(redshift_threshold) * 60000000}
                ORDER BY r.duration DESC
                LIMIT {TOP_N}
            """
            with st.spinner("Carregando lista de queries..."):
                df_list = run_redshift(sql_list)
            st.caption(f"✅ Lista carregada (Top {TOP_N})")

            if not df_list.empty:
                st.dataframe(df_list, use_container_width=True, height=380, hide_index=True)

    except Exception as e:
        st.exception(e)

# ======================== CARTÃO 2 — Power BI ========================
with col2:
    st.subheader("Power BI — Último refresh (Painel CD)")
    try:
        with st.spinner("Conectando ao Postgres..."):
            _ = get_postgres_conn()
        st.caption("✅ Conexão Postgres OK")
        sql_refresh = """
            SELECT TO_CHAR(
                     DATE_TRUNC('minute', MAX(backlog_sap.etl_load_date AT TIME ZONE 'UTC')),
                     'YYYY-MM-DD HH24:MI:SS'
                   ) AS backlog_sap_refreshed_at
            FROM robos_bi.mv_backlog_sap AS backlog_sap;
        """
        try:
            with st.spinner("Lendo último refresh..."):
                df_ref = run_postgres(sql_refresh)
        except Exception:
            try:
                get_postgres_conn.clear()
            except Exception:
                pass
            _ = get_postgres_conn()
            with st.spinner("Lendo último refresh..."):
                df_ref = run_postgres(sql_refresh)
        st.caption("✅ Refresh lido")

        if df_ref.empty or pd.isna(df_ref.loc[0, "backlog_sap_refreshed_at"]):
            st.warning("Não encontrei registro de refresh para o backlog_sap.")
        else:
            last_refresh_str = str(df_ref.loc[0, "backlog_sap_refreshed_at"]).strip()
            last_refresh_utc = pd.to_datetime(last_refresh_str, utc=True, errors="coerce")
            if pd.isna(last_refresh_utc):
                st.error(f"Formato de data inesperado: {last_refresh_str!r}")
            else:
                now_utc = datetime.now(timezone.utc)
                age_min = int((now_utc - last_refresh_utc).total_seconds() // 60)
                st.metric(
                    label="Último refresh — backlog_sap (UTC)",
                    value=last_refresh_utc.strftime("%d/%m/%Y %H:%M:%S"),
                   delta=f"{age_min} min atrás"
                )

                if age_min > int(refresh_alert_min):
                    st.error(f"Atraso acima do limite configurado ({age_min} min).")
                else:
                    st.success("Refresh dentro do limite.")

    except Exception as e:
        st.error(f"Falha ao obter último refresh: {e}")

# ======================== CARTÃO 3 — Jira ========================
with col3:
    st.subheader("Jira — Chamados abertos")

    @st.cache_data(ttl=60, show_spinner=False)
    def jira_approx_count(jql: str) -> int:
        s = st.secrets["jira"]
        base = s["base_url"].rstrip("/")
        url = f"{base}/rest/api/3/search/approximate-count"
        resp = requests.post(
            url,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            json={"jql": jql},
            auth=(s["email"], s["api_token"]),
            timeout=12,
        )
        resp.raise_for_status()
        return int(resp.json().get("count", 0))

    @st.cache_data(ttl=60, show_spinner=False)
    def jira_fetch_issues(jql: str, max_results: int = 20) -> list[dict]:
        s = st.secrets["jira"]
        base = s["base_url"].rstrip("/")
        url = f"{base}/rest/api/3/search/jql"
        payload = {
            "jql": jql + " ORDER BY updated DESC",
            "maxResults": max_results,
            "fields": ["summary", "status", "assignee", "updated"]
        }
        resp = requests.post(
            url,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            json=payload,
            auth=(s["email"], s["api_token"]),
            timeout=12,
        )
        resp.raise_for_status()
        return resp.json().get("issues", [])

    try:
        jql_td_open = (
            "project = TD "
            "AND resolution IS EMPTY "
            "AND statusCategory IN ('To Do','In Progress') "
            "AND (assignee = currentUser() OR assignee IS EMPTY)"
        )
        with st.spinner("Consultando Jira..."):
            total_abertos = jira_approx_count(jql_td_open)
            issues = jira_fetch_issues(jql_td_open, max_results=20)

        st.metric("Chamados Abertos", total_abertos)

        if issues:
            base_url = st.secrets["jira"]["base_url"].rstrip("/")
            rows = []
            for it in issues:
                f = it.get("fields", {})
                rows.append({
                    "Chamado": it.get("key"),
                    "Resumo": str(f.get("summary", "")).strip(),
                    "Status": (f.get("status") or {}).get("name"),
                    "Responsável": (f.get("assignee") or {}).get("displayName") or "—",
                    "Atualizado": f.get("updated"),
                    "URL": f"{base_url}/browse/{it.get('key')}",
                })
            df = pd.DataFrame(rows)

            df["Atualizado"] = pd.to_datetime(df["Atualizado"], utc=True, errors="coerce") \
                                  .dt.tz_convert(TZ).dt.strftime("%d/%m %H:%M")

            try:
                st.dataframe(
                    df[["Chamado", "Resumo", "Status", "Responsável", "Atualizado", "URL"]],
                    use_container_width=True,
                    height=360,
                    hide_index=True,
                    column_config={
                        "URL": st.column_config.LinkColumn("Abrir", help="Abrir no Jira"),
                        "Resumo": st.column_config.TextColumn("Resumo", width="large", max_chars=120),
                    },
                )
            except Exception:
                st.dataframe(
                    df[["Chamado", "Resumo", "Status", "Responsável", "Atualizado", "URL"]],
                    use_container_width=True, height=360, hide_index=True
                )

            import urllib.parse as _u
            jql_enc = _u.quote(jql_td_open, safe="")
            st.caption(f"[Ver tudo no Jira]({base_url}/issues/?jql={jql_enc})")
        else:
            st.caption("Nenhum chamado aberto encontrado.")

    except requests.HTTPError as e:
        st.error(f"HTTP {e.response.status_code} ao consultar Jira")
        try:
            st.code(e.response.text)
        except Exception:
            pass
    except Exception as e:
        st.error(f"Falha ao consultar Jira: {e}")

# ======================== CONFIG ========================
TZ = ZoneInfo("America/Sao_Paulo")
FIRST_ORDER_MAGENTO = "2023-06-20"
TAX_RATING = 0.634

st.set_page_config(page_title="KPIs Evino (flash do dia/mês)", page_icon="📊", layout="wide")

# ======================== FUNÇÕES AUXILIARES ========================
def get_now_kestra_style() -> datetime:
    now = datetime.now(tz=TZ)
    if now.hour == 0 and now.minute < 30:
        now = now - timedelta(minutes=now.minute + 1)
    return now

def _as_date_str_local(d: datetime) -> str:
    return d.astimezone(TZ).strftime("%Y-%m-%d")

def _as_minute_str_utc(d: datetime) -> str:
    return d.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")

def _to_tz_aware_utc(x) -> pd.Timestamp | None:
    if x is None:
        return None
    ts = pd.to_datetime(x, errors="coerce", utc=True)
    if ts is None or pd.isna(ts):
        return None
    return ts

def _fmt_sampa(ts: pd.Timestamp | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    if ts is None:
        return "—"
    return ts.tz_convert(TZ).strftime(fmt)

def _kfmt(v):
    return f"{v/1000:,.1f}k".replace(",", "X").replace(".", ",").replace("X", ".")

def _pct(v):
    return f"{100*v:0.1f}%"

@st.cache_resource(show_spinner=False)
def get_redshift_conn():
    s = st.secrets["dw_vissimo"]
    return psycopg2.connect(
        host=s["host"],
        port=s.get("port", 5439),
        dbname=s["dbname"],
        user=s["user"],
        password=s["password"],
        connect_timeout=8,
        application_name="MonitorDW"
    )

# ======================== KPIs ========================
@st.cache_data(ttl=60, show_spinner=False)
def kpi_get_today_revenue(now_dt: datetime) -> dict:
    today_local = _as_date_str_local(now_dt)
    last_hour_utc = _as_minute_str_utc(now_dt - timedelta(hours=1))
    sql = f"""
    SELECT
      SUM(CASE
            WHEN fo.payment_method = 'evino_adyen_boleto' THEN (fo.price_to_pay+fo.item_shipping_amount)*0.65
            WHEN fo.payment_method = 'evino_adyen_pix'    THEN (fo.price_to_pay+fo.item_shipping_amount)*0.75
            ELSE (fo.price_to_pay+fo.item_shipping_amount)
          END) AS revenue,
      SUM(CASE
            WHEN fo.payment_method = 'evino_adyen_boleto' THEN fo.cm2_realized*0.65
            WHEN fo.payment_method = 'evino_adyen_pix'    THEN fo.cm2_realized*0.75
            ELSE fo.cm2_realized
          END)
        / NULLIF(SUM(CASE
            WHEN fo.payment_method = 'evino_adyen_boleto' THEN (fo.price_to_pay+fo.item_shipping_amount)*0.65
            WHEN fo.payment_method = 'evino_adyen_pix'    THEN (fo.price_to_pay+fo.item_shipping_amount)*0.75
            ELSE (fo.price_to_pay+fo.item_shipping_amount)
          END), 0) AS cm1,
      SUM(CASE
            WHEN fo.payment_method = 'evino_adyen_boleto' THEN fo.cm2_realized*0.65
            WHEN fo.payment_method = 'evino_adyen_pix'    THEN fo.cm2_realized*0.75
            ELSE fo.cm2_realized
          END)
        / NULLIF(SUM(CASE
            WHEN fo.payment_method = 'evino_adyen_boleto' THEN (fo.price_to_pay+fo.item_shipping_amount)*0.65
            WHEN fo.payment_method = 'evino_adyen_pix'    THEN (fo.price_to_pay+fo.item_shipping_amount)*0.75
            ELSE (fo.price_to_pay+fo.item_shipping_amount)
          END), 0) AS cm2,
      SUM(CASE WHEN fo.created_at_datetime > '{last_hour_utc}' THEN
            CASE
              WHEN fo.payment_method = 'evino_adyen_boleto' THEN (fo.price_to_pay+fo.item_shipping_amount)*0.65
              WHEN fo.payment_method = 'evino_adyen_pix'    THEN (fo.price_to_pay+fo.item_shipping_amount)*0.75
              ELSE (fo.price_to_pay+fo.item_shipping_amount)
            END
          END) AS last_hour_revenue,
      MAX(created_at_datetime) AS last_order_created_at
    FROM dora_red_aggregations.ev_fact_order_item fo
    WHERE DATE(fo.created_at_datetime) = '{today_local}'
      AND COALESCE(UPPER(fo.voucher_code), '') NOT ILIKE 'TV%%'
      AND fo.is_solid = 1
      AND fo.platform <> 'vivino'
    """
    with get_redshift_conn() as conn:
        df = pd.read_sql(sql, conn).fillna(0)

    if "last_order_created_at" in df.columns:
        df["last_order_created_at"] = pd.to_datetime(df["last_order_created_at"], utc=True)

    last_order = _to_tz_aware_utc(df.at[0, "last_order_created_at"]) if df.at[0, "last_order_created_at"] else None

    return {
        "today_revenue": float(df.at[0, "revenue"] or 0),
        "cm1": float(df.at[0, "cm1"] or 0),
        "cm2": float(df.at[0, "cm2"] or 0),
        "last_hour_revenue": float(df.at[0, "last_hour_revenue"] or 0),
        "last_order_created_at": last_order,
    }

@st.cache_data(ttl=60, show_spinner=False)
def kpi_get_top_seller(now_dt: datetime, last_order_created_at: pd.Timestamp | None) -> dict:
    if last_order_created_at is None:
        return {"top_seller": "---", "bottles": 0}

    today_local = _as_date_str_local(now_dt)
    last_hour_utc = (last_order_created_at - pd.Timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")
    sql = f"""
    SELECT
      dp.name AS top_seller,
      SUM(qty_ordered) AS bottles
    FROM dora_red_aggregations.ev_fact_order_item foi
    JOIN dora_red_aggregations.ev_dim_product dp ON foi.sku = dp.sku
    WHERE foi.is_solid = 1
      AND foi.is_wine = 1
      AND DATE(foi.created_at_datetime) = '{today_local}'
      AND foi.created_at_datetime > '{last_hour_utc}'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 1
    """
    with get_redshift_conn() as conn:
        df = pd.read_sql(sql, conn).fillna(0)

    if len(df) == 0:
        return {"top_seller": "---", "bottles": 0}

    return {"top_seller": str(df.at[0, "top_seller"]), "bottles": int(df.at[0, "bottles"])}

@st.cache_data(ttl=60, show_spinner=False)
def kpi_get_month_flash(now_dt: datetime) -> dict:
    first_day_local = now_dt.astimezone(TZ).strftime("%Y-%m-01")
    today_local = _as_date_str_local(now_dt)
    if today_local[:7] == FIRST_ORDER_MAGENTO[:7]:
        first_day_local = FIRST_ORDER_MAGENTO

    sql = f"""
    SELECT
      SUM(fo.price_to_pay+fo.item_shipping_amount) AS revenue_flash_sale,
      SUM(cm1_realized) / NULLIF(SUM(fo.price_to_pay+fo.item_shipping_amount), 0) AS cm1,
      SUM(cm2_realized) / NULLIF(SUM(fo.price_to_pay+fo.item_shipping_amount), 0) AS cm2
    FROM dora_red_aggregations.ev_fact_order_item fo
    WHERE DATE(created_at_datetime) BETWEEN '{first_day_local}' AND '{today_local}'
      AND COALESCE(UPPER(fo.voucher_code), '') NOT ILIKE 'TV%%'
      AND fo.platform <> 'vivino'
      AND is_solid = 1
    """
    with get_redshift_conn() as conn:
        df = pd.read_sql(sql, conn).fillna(0)

    return {
        "month_revenue_flash_sale": float(df.at[0, "revenue_flash_sale"] or 0),
        "month_cm1": float(df.at[0, "cm1"] or 0),
        "month_cm2": float(df.at[0, "cm2"] or 0),
    }

@st.cache_data(ttl=60, show_spinner=False)
def kpi_get_forecast(now_dt: datetime, last_order_created_at: pd.Timestamp | None) -> dict:
    if last_order_created_at is None:
        return {"expected_percentage": 0.0, "today_forecast": 0.0}

    today_local = _as_date_str_local(now_dt)
    first_day_comp = (now_dt.date() - timedelta(days=42)).strftime("%Y-%m-%d")
    last_day_comp  = (now_dt.date() - timedelta(days=7)).strftime("%Y-%m-%d")
    current_time_hhmm = last_order_created_at.strftime("%H%M")

    sql = f"""
    WITH hist AS (
      SELECT
        TO_CHAR(DATE(created_at_datetime), 'YYYY-MM-DD') AS d,
        SUM(CASE WHEN TO_CHAR(created_at_datetime, 'HH24MI') <= '{current_time_hhmm}'
                 THEN (fo.price_to_pay+fo.item_shipping_amount) END) AS partial_revenue,
        SUM( (fo.price_to_pay+fo.item_shipping_amount) ) AS total_revenue
      FROM dora_red_aggregations.ev_fact_order_item fo
      WHERE DATE(created_at_datetime) BETWEEN '{FIRST_ORDER_MAGENTO}' AND '{last_day_comp}'
        AND DATE(created_at_datetime) >= '{first_day_comp}'
        AND is_solid = 1
        AND DATE_PART(dayofweek, DATE(fo.created_at_datetime)) = EXTRACT(DOW FROM DATE('{today_local}'))
      GROUP BY 1
    )
    SELECT
      AVG(partial_revenue/NULLIF(total_revenue,0)) AS expected_percentage,
      (SELECT rev_lastclick_plan FROM dora_red_aggregations.vw_ev_mkt_forecast f WHERE f.date = '{today_local}') AS today_forecast
    FROM hist
    """
    with get_redshift_conn() as conn:
        df = pd.read_sql(sql, conn).fillna(0)

    return {
        "expected_percentage": float(df.at[0, "expected_percentage"] or 0),
        "today_forecast": float(df.at[0, "today_forecast"] or 0),
    }

@st.cache_data(ttl=60, show_spinner=False)
def kpi_get_month_forecast(now_dt: datetime) -> dict:
    yesterday_local = (now_dt - timedelta(days=1)).astimezone(TZ).strftime("%Y-%m-%d")
    first_day_local = now_dt.astimezone(TZ).strftime("%Y-%m-01")
    aux_day = date(now_dt.year, now_dt.month, 28) + timedelta(days=4)
    last_day_local = (aux_day - timedelta(days=aux_day.day)).strftime("%Y-%m-%d")

    sql = f"""
    SELECT
      SUM(CASE WHEN DATE(date) BETWEEN '{first_day_local}' AND '{yesterday_local}' THEN rev_lastclick_plan END) AS forecast_until_yesterday,
      SUM(rev_lastclick_plan) AS month_forecast
    FROM dora_red_aggregations.vw_ev_mkt_forecast
    WHERE DATE(date) BETWEEN '{first_day_local}' AND '{last_day_local}'
    """
    with get_redshift_conn() as conn:
        df = pd.read_sql(sql, conn).fillna(0)

    return {
        "forecast_until_yesterday": float(df.at[0, "forecast_until_yesterday"] or 0),
        "month_forecast": float(df.at[0, "month_forecast"] or 0),
    }

# ======================== LAYOUT ========================
with st.container():
    st.subheader("📊 KPIs Evino (flash do dia/mês)")

    now = get_now_kestra_style()

    today  = kpi_get_today_revenue(now)
    month  = kpi_get_month_flash(now)
    top    = kpi_get_top_seller(now, today.get("last_order_created_at"))
    fc_day = kpi_get_forecast(now, today.get("last_order_created_at"))
    fc_mon = kpi_get_month_forecast(now)

    expected_revenue = fc_day["today_forecast"] * (fc_day["expected_percentage"] or 0)
    expected_month_revenue = fc_mon["forecast_until_yesterday"] + expected_revenue
    hora_pedido = _fmt_sampa(today.get("last_order_created_at"))

    diff_min = (
        (now - today["last_order_created_at"].astimezone(TZ)).total_seconds() / 60
        if today["last_order_created_at"] else None
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Hoje (Receita)", _kfmt(today["today_revenue"]), delta=_kfmt(today["last_hour_revenue"]))
        st.caption(f"CM2 hoje: {_pct(today['cm2'])}")
        st.caption(f"Top 1h: {top['top_seller']} ({top['bottles']} garrafas)")
        st.caption(f"Hora do último pedido: {hora_pedido}")

    with col2:
        st.metric("Mês — Flash", _kfmt(month["month_revenue_flash_sale"]))
        st.caption(f"CM2 mês: {_pct(month['month_cm2'])}")

    with col3:
        st.metric("Meta parcial/total (dia)", f"{_kfmt(expected_revenue)} / {_kfmt(fc_day['today_forecast'])}")
        st.caption(f"Meta parcial/total (mês): {_kfmt(expected_month_revenue)} / {_kfmt(fc_mon['month_forecast'])}")
    st.divider()

    if diff_min is not None and diff_min > 30:
        st.warning(f"⚠️ Painel pode estar defasado — último pedido foi há {diff_min:.0f} minutos")
    else:
        st.success(f"📦 Último pedido atualizado há {diff_min:.0f} minutos")

    st.caption(f"📅 Snapshot gerado em: {now.strftime('%Y-%m-%d %H:%M:%S')}") 
    
WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/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")

# SLACK ALERTA
WEBHOOK_URL = os.getenv(
    "SLACK_WEBHOOK_URL",
    "https://hooks.slack.com/services/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"
).strip()

def slack_post(text: str = "", blocks: list | None = None, webhook_url: str | None = None, timeout: int = 15):
    url = (webhook_url or WEBHOOK_URL or "").strip()
    if not url or "hooks.slack.com" not in url:
        return False, "WEBHOOK_URL não configurado"

    payload = {"text": text or ""}
    if blocks:
        payload["blocks"] = blocks

    backoff = 1.0
    last_err = "desconhecido"
    for _ in range(4):
        resp = requests.post(url, headers={"Content-type": "application/json"},
                             data=json.dumps(payload), timeout=timeout)
        if resp.status_code in (200, 204):
            return True, "ok"
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "1"))
            time.sleep(retry_after + 1)
            last_err = f"429 rate-limited. body={resp.text[:200]}"
            continue
        if 500 <= resp.status_code < 600:
            last_err = f"{resp.status_code} server error. body={resp.text[:200]}"
            time.sleep(backoff)
            backoff = min(backoff * 2, 8)
            continue
        return False, f"{resp.status_code} client error. body={resp.text[:200]}"
    return False, last_err

def _fmt_sql_list(df_list: pd.DataFrame, max_items: int = 5) -> str:
    if df_list is None or df_list.empty:
        return "—"
    linhas = []
    cols = set(df_list.columns.str.lower())
    for _, r in df_list.head(max_items).iterrows():
        pid = r.get("pid", r.get("PID", "—"))
        user = r.get("user_name", r.get("USER_NAME", "—"))
        durm = r.get("duration_minutes", r.get("DURATION_MINUTES", None))
        dur_str = f"{float(durm):.1f}m" if durm is not None else "—"
        q = str(r.get("query", r.get("QUERY", ""))).replace("\n", " ").strip()
        q = q[:120]
        linhas.append(f"• `pid:{pid}` ({user}, {dur_str}) — {q}")
    return "\n".join(linhas) if linhas else "—"

def _fmt_jira_list(rows: list[dict], base_url: str | None = None, max_items: int = 6) -> str:
    if not rows:
        return "—"
    out = []
    for r in rows[:max_items]:
        key = r.get("Chamado", "—")
        url = r.get("URL")
        key_md = f"<{url}|{key}>" if url else f"{key}"
        resumo = str(r.get("Resumo", "—")).strip()[:80]
        status = r.get("Status", "—")
        resp = r.get("Responsável", "—")
        upd = r.get("Atualizado", "—")
        out.append(f"• {key_md} — *{status}* — {resp} — {upd}\n  _{resumo}_")
    return "\n".join(out)

def build_unified_blocks(
    running_over: int,
    df_list: pd.DataFrame,
    redshift_threshold: int,
    powerbi_status: str,           
    last_refresh_utc,             
    age_min,                       
    jira_total: int,             
    jira_rows: list[dict] | None = None,
    refresh_alert_min: int | None = None,
    jira_filter_url: str | None = None,
    redshift_console_url: str | None = None,
) -> list[dict]:
    now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")

    # Campos (somente resumo, sem listas)
    redshift_line = f"*Queries > {redshift_threshold} min:* {running_over}"
    def _fmt_pb_local(ts_utc):
        if ts_utc is None:
            return "—"
        try:
         return ts_utc.tz_convert(TZ).strftime('%d/%m/%Y %H:%M:%S')
        except Exception:
            return "-"

    pb_line = f"*Power BI — Último refresh (UTC):* {_fmt_pb_local(last_refresh_utc)}"

    jira_line = f"*Chamados abertos:* {jira_total}"

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "📣 Monitor DW — Status agora"}},
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": redshift_line},
                {"type": "mrkdwn", "text": pb_line},
                {"type": "mrkdwn", "text": jira_line},
            ],
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"⏱️ {now_str} • Monitor DW"}]},
    ]
    return blocks

# Coleta dos dados calculados nos cards 
running_over = locals().get("running_over", 0)
df_list = locals().get("df_list", pd.DataFrame())
redshift_threshold = int(locals().get("redshift_threshold", REDSHIFT_THRESHOLD_MIN))

powerbi_status = locals().get("status_atual", "unknown")
last_refresh_utc = locals().get("last_refresh_utc", None)
age_min = locals().get("age_min", None)
refresh_alert_min = int(locals().get("refresh_alert_min", REFRESH_ALERT_MIN))

jira_total = locals().get("total_abertos", 0)
jira_rows = locals().get("rows", [])
try:
    _base_url = st.secrets["jira"]["base_url"].rstrip("/")
    import urllib.parse as _u
    _jql = (
        "project = TD AND resolution IS EMPTY AND statusCategory IN ('To Do','In Progress') "
        "AND (assignee = currentUser() OR assignee IS EMPTY)"
    )
    jira_filter_url = f"{_base_url}/issues/?jql={_u.quote(_jql, safe='')}"
except Exception:
    jira_filter_url = None

redshift_console_url = os.getenv("REDSHIFT_CONSOLE_URL", "").strip() or None

#Parâmetros de regra
KPI_ALERT_PCT = float(os.getenv("KPI_ALERT_PCT", "0.20"))  # 20%

# Coleta dos dados dos cards 
running_over = int(locals().get("running_over", 0))  
df_list = locals().get("df_list", pd.DataFrame())
redshift_threshold = int(locals().get("redshift_threshold", REDSHIFT_THRESHOLD_MIN))  

powerbi_status = str(locals().get("status_atual", "unknown"))  
last_refresh_utc = locals().get("last_refresh_utc", None)     
age_min = locals().get("age_min", None)                      
refresh_alert_min = int(locals().get("refresh_alert_min", REFRESH_ALERT_MIN))

jira_total = int(locals().get("total_abertos", 0))
jira_rows = locals().get("rows", [])

# KPIs Evino — percentual do dia 
kpi_evino_pct = locals().get("kpi_evino_pct", None) 
try:
    _base_url = st.secrets["jira"]["base_url"].rstrip("/")
    import urllib.parse as _u
    _jql = (
        "project = TD AND resolution IS EMPTY AND statusCategory IN ('To Do','In Progress') "
        "AND (assignee = currentUser() OR assignee IS EMPTY)"
    )
    jira_filter_url = f"{_base_url}/issues/?jql={_u.quote(_jql, safe='')}"
except Exception:
    jira_filter_url = None

redshift_console_url = os.getenv("REDSHIFT_CONSOLE_URL", "").strip() or None

#Regras de erros
def has_query_anomaly(running_over: int, threshold: int) -> bool:
   
    return (running_over is not None) and (running_over >= 1) and (threshold >= 10)

def has_powerbi_anomaly(last_refresh_utc: pd.Timestamp | None) -> bool:
    if last_refresh_utc is None:
        return True
    now_utc = datetime.now(timezone.utc)
    return last_refresh_utc.hour != now_utc.hour

def has_jira_anomaly(jira_total: int) -> bool:
    return jira_total > 0

def has_kpi_anomaly(kpi_evino_pct, kpi_min: float) -> bool:
    if kpi_evino_pct is None:
        return False 
    return float(kpi_evino_pct) < float(kpi_min)

query_bad   = has_query_anomaly(running_over, redshift_threshold)
powerbi_bad = has_powerbi_anomaly(last_refresh_utc)
jira_bad    = has_jira_anomaly(jira_total)
kpi_bad     = has_kpi_anomaly(kpi_evino_pct, KPI_ALERT_PCT)
any_bad = query_bad or powerbi_bad or jira_bad or kpi_bad

#Formatação auxiliar 
def _fmt_sql_list(df_list: pd.DataFrame, max_items: int = 5) -> str:
    if df_list is None or df_list.empty:
        return "—"
    linhas = []
    for _, r in df_list.head(max_items).iterrows():
        pid = r.get("pid", r.get("PID", "—"))
        user = r.get("user_name", r.get("USER_NAME", "—"))
        durm = r.get("duration_minutes", r.get("DURATION_MINUTES", None))
        dur_str = f"{float(durm):.1f}m" if durm is not None else "—"
        q = str(r.get("query", r.get("QUERY", ""))).replace("\n", " ").strip()
        q = q[:120]
        linhas.append(f"• `pid:{pid}` ({user}, {dur_str}) — {q}")
    return "\n".join(linhas) if linhas else "—"

def _fmt_jira_list(rows: list[dict], base_url: str | None = None, max_items: int = 6) -> str:
    if not rows:
        return "—"
    out = []
    for r in rows[:max_items]:
        key = r.get("Chamado", "—")
        url = r.get("URL")
        key_md = f"<{url}|{key}>" if url else f"{key}"
        resumo = str(r.get("Resumo", "—")).strip()[:80]
        status = r.get("Status", "—")
        resp = r.get("Responsável", "—")
        upd = r.get("Atualizado", "—")
        out.append(f"• {key_md} — *{status}* — {resp} — {upd}\n  _{resumo}_")
    return "\n".join(out)

def _fmt_pb_utc(ts_utc):
    if ts_utc is None:
        return "—"
    try:
        return pd.to_datetime(ts_utc, utc=True).strftime('%d/%m/%Y %H:%M:%S UTC')
    except Exception:
        return "—"
#  Blocks
now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
blocks = [
    {"type": "header", "text": {"type": "plain_text", "text": "🚨 Monitor DW — Errors"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": f"⏱️ {now_str} • Enviado automaticamente"}]},
    {"type": "divider"},
]
# Queries
if query_bad:
    blocks += [
        {"type": "section", "text": {"type": "mrkdwn",
         "text": f"*Queries acima de {redshift_threshold} min:* {running_over} (limiar: ≥2)"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": _fmt_sql_list(df_list)}},
    ]
    actions = []
    if redshift_console_url:
        actions.append({"type": "button", "text": {"type": "plain_text", "text": "Abrir Redshift"}, "url": redshift_console_url})
    if actions:
        blocks.append({"type": "actions", "elements": actions})
    blocks.append({"type": "divider"})
# Power BI (CD)
if powerbi_bad:
    blocks += [
        {"type": "section", "text": {"type": "mrkdwn",
         "text": f"*Power BI — Último refresh (UTC):* { _fmt_pb_utc(last_refresh_utc) }  \n*Status:* Refresh **não realizado** nesta hora."}},
        {"type": "divider"},
    ]
# Jira
if jira_bad:
    blocks += [
        {"type": "section", "text": {"type": "mrkdwn",
         "text": f"*Chamados abertos (Jira):* {jira_total}"}},
        {"type": "section", "text": {"type": "mrkdwn",
         "text": _fmt_jira_list(jira_rows)}},
    ]
    actions = []
    if jira_filter_url:
        actions.append({"type": "button", "text": {"type": "plain_text", "text": "Abrir filtro no Jira"}, "url": jira_filter_url})
    if actions:
        blocks.append({"type": "actions", "elements": actions})
    blocks.append({"type": "divider"})
# KPIs Evino
if kpi_bad:
    pct_txt = f"{(float(kpi_evino_pct) * 100):.1f}%" if kpi_evino_pct is not None else "—"
    limiar_txt = f"{(float(KPI_ALERT_PCT) * 100):.0f}%"
    blocks += [
        {"type": "section", "text": {"type": "mrkdwn",
         "text": f"*KPI Evino (dia):* {pct_txt}  •  *Limiar:* {limiar_txt}"}},
        {"type": "divider"},
    ]
# --------- De-dup e envio: só envia se houver anomalia ---------
if any_bad:
    digest_data = {
        "q": running_over if query_bad else 0,
        "pb_age": age_min if powerbi_bad else 0,
        "jira": jira_total if jira_bad else 0,
        "kpi": round(float(kpi_evino_pct or 0), 4) if kpi_bad else 1.0,
        "t": datetime.now(TZ).replace(minute=(datetime.now(TZ).minute // 15) * 15, second=0, microsecond=0).isoformat(),
    }
    digest = json.dumps(digest_data, ensure_ascii=False, sort_keys=True)

    if "last_alert_digest" not in st.session_state or st.session_state["last_alert_digest"] != digest:
        ok, info = slack_post(text="🚨 Monitor DW — errors", blocks=blocks)
        st.caption(f"🔔 Alerta Slack: {'ok' if ok else 'falhou'} — {info}")
        st.session_state["last_alert_digest"] = digest
    else:
        st.caption("🔕 Sem mudanças relevantes nas erros; nenhum novo alerta enviado.")
else:
    st.caption("✅ Sem erros ; nenhum alerta enviado ao Slack.")
