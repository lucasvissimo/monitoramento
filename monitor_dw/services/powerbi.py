# -*- coding: utf-8 -*-
"""
Serviço de monitoramento do Power BI
"""

import pandas as pd
import streamlit as st
from ..db import run_postgres
from ..config import TZ
from datetime import datetime, timezone


def get_last_refresh() -> tuple[pd.Timestamp | None, int | None]:
    """
    Obtém último refresh do backlog_sap
    Retorna: (timestamp_utc, age_minutes)
    """
    try:
        sql_refresh = """
            SELECT TO_CHAR(
                     DATE_TRUNC('minute', MAX(backlog_sap.etl_load_date AT TIME ZONE 'UTC')),
                     'YYYY-MM-DD HH24:MI:SS'
                   ) AS backlog_sap_refreshed_at
            FROM robos_bi.mv_backlog_sap AS backlog_sap;
        """
        df_ref = run_postgres(sql_refresh)
        
        if df_ref.empty or pd.isna(df_ref.loc[0, "backlog_sap_refreshed_at"]):
            return None, None
        
        last_refresh_str = str(df_ref.loc[0, "backlog_sap_refreshed_at"]).strip()
        last_refresh_utc = pd.to_datetime(last_refresh_str, utc=True, errors="coerce")
        
        if pd.isna(last_refresh_utc):
            return None, None
        
        now_utc = datetime.now(timezone.utc)
        age_min = int((now_utc - last_refresh_utc).total_seconds() // 60)
        
        return last_refresh_utc, age_min
        
    except Exception:
        return None, None


def has_powerbi_anomaly(last_refresh_utc: pd.Timestamp | None) -> bool:
    """
    Verifica se há anomalia no Power BI baseado no último refresh
    Considera como atualizado se o refresh foi no mesmo dia (UTC)
    """
    if last_refresh_utc is None:
        return True
    
    # Usar UTC para comparação direta - converter para pandas Timestamp
    now_utc = pd.Timestamp.now(tz='UTC')
    
    # Verificar se o refresh foi no mesmo dia (UTC)
    # Se o refresh foi no mesmo dia (mesmo ano, mês e dia), considera OK
    if (now_utc.year == last_refresh_utc.year and 
        now_utc.month == last_refresh_utc.month and 
        now_utc.day == last_refresh_utc.day):
        return False
    
    # Se não foi no mesmo dia, considera atrasado
    return True


def get_refresh_status_info(last_refresh_utc: pd.Timestamp | None) -> dict:
    """
    Obtém informações detalhadas sobre o status do refresh
    """
    if last_refresh_utc is None:
        return {
            "status": "error",
            "message": "Não encontrei registro de refresh para o backlog_sap",
            "last_refresh_utc": None,
            "last_refresh_local": None,
            "age_minutes": None,
            "is_anomaly": True
        }
    
    # Converter para timezone local
    last_refresh_local = last_refresh_utc.tz_convert(TZ)
    now_local = datetime.now(TZ)
    diff_minutes = (now_local - last_refresh_local).total_seconds() / 60
    
    is_anomaly = has_powerbi_anomaly(last_refresh_utc)
    
    if is_anomaly:
        status = "warning"
        message = f"Refresh atrasado - última atualização foi há {diff_minutes:.0f} minutos"
    else:
        status = "success"
        message = f"Refresh dentro do prazo (última atualização há {diff_minutes:.0f} minutos)"
    
    return {
        "status": status,
        "message": message,
        "last_refresh_utc": last_refresh_utc,
        "last_refresh_local": last_refresh_local,
        "age_minutes": diff_minutes,
        "is_anomaly": is_anomaly
    }
