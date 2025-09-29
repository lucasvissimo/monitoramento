# -*- coding: utf-8 -*-
"""
Serviço de KPIs da Evino
"""

import pandas as pd
import streamlit as st
from ..db import get_redshift_conn
from ..config import (
    TZ, FIRST_ORDER_MAGENTO, _as_date_str_local, _as_minute_str_utc, 
    _to_tz_aware_utc, _fmt_sampa, _kfmt, _pct, get_now_kestra_style
)
from datetime import datetime, timedelta


@st.cache_data(ttl=60, show_spinner=False)
def kpi_get_today_revenue(now_dt: datetime) -> dict:
    """Obtém receita do dia atual"""
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
    """Obtém top seller da última hora"""
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

    return {"top_seller": str(df.at[0, "top_seller"]), "bottles": int(df.at[0, "bottles"]) }


@st.cache_data(ttl=60, show_spinner=False)
def kpi_get_month_flash(now_dt: datetime) -> dict:
    """Obtém receita flash do mês"""
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
    """Obtém forecast do dia"""
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
    """Obtém forecast do mês"""
    yesterday_local = (now_dt - timedelta(days=1)).astimezone(TZ).strftime("%Y-%m-%d")
    first_day_local = now_dt.astimezone(TZ).strftime("%Y-%m-01")
    aux_day = datetime(now_dt.year, now_dt.month, 28).date() + timedelta(days=4)
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


def get_all_kpis() -> dict:
    """Obtém todos os KPIs de uma vez"""
    try:
        now = get_now_kestra_style()
        
        # Obter dados com tratamento de erro individual
        try:
            today = kpi_get_today_revenue(now)
        except Exception as e:
            st.error(f"Erro ao obter receita do dia: {e}")
            today = {}
        
        try:
            month = kpi_get_month_flash(now)
        except Exception as e:
            st.error(f"Erro ao obter dados mensais: {e}")
            month = {}
        
        try:
            top = kpi_get_top_seller(now, today.get("last_order_created_at"))
        except Exception as e:
            st.error(f"Erro ao obter top seller: {e}")
            top = {}
        
        try:
            fc_day = kpi_get_forecast(now, today.get("last_order_created_at"))
        except Exception as e:
            st.error(f"Erro ao obter forecast do dia: {e}")
            fc_day = {}
        
        try:
            fc_mon = kpi_get_month_forecast(now)
        except Exception as e:
            st.error(f"Erro ao obter forecast mensal: {e}")
            fc_mon = {}
        
        # Calcular valores derivados com tratamento de erro
        try:
            expected_revenue = fc_day.get("today_forecast", 0) * (fc_day.get("expected_percentage", 0) or 0)
        except Exception:
            expected_revenue = 0
        
        try:
            expected_month_revenue = fc_mon.get("forecast_until_yesterday", 0) + expected_revenue
        except Exception:
            expected_month_revenue = 0
        
        try:
            hora_pedido = _fmt_sampa(today.get("last_order_created_at"))
        except Exception:
            hora_pedido = "N/A"
        
        try:
            diff_min = (
                (now - today["last_order_created_at"].astimezone(TZ)).total_seconds() / 60
                if today.get("last_order_created_at") else None
            )
        except Exception:
            diff_min = None
        
        return {
            "today": today,
            "month": month,
            "top": top,
            "fc_day": fc_day,
            "fc_mon": fc_mon,
            "expected_revenue": expected_revenue,
            "expected_month_revenue": expected_month_revenue,
            "hora_pedido": hora_pedido,
            "diff_min": diff_min,
            "now": now
        }
    except Exception as e:
        st.error(f"Erro geral ao obter KPIs: {e}")
        return {}
