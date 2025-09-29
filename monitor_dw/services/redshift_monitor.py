# -*- coding: utf-8 -*-
"""
Serviço de monitoramento do Redshift
"""

import pandas as pd
import streamlit as st
from ..db import run_redshift, get_redshift_conn
from ..config import TZ
from datetime import datetime


def get_queries_over_threshold(threshold_min: int) -> int:
    """Obtém contagem de queries acima do threshold"""
    try:
        sql_count = f"""
            SELECT COUNT(*) AS running_over
            FROM stv_recents
            WHERE status = 'Running'
              AND duration > {int(threshold_min) * 60000000}
        """
        df_count = run_redshift(sql_count)
        return int(df_count.iloc[0]["running_over"]) if not df_count.empty else 0
    except Exception:
        return 0


def get_queries_list(threshold_min: int, limit: int = 20) -> pd.DataFrame:
    """Obtém lista de queries acima do threshold"""
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
          AND r.duration > {int(threshold_min) * 60000000}
        ORDER BY r.duration DESC
        LIMIT {limit}
    """
    return run_redshift(sql_list)


def get_schemas() -> list[str]:
    """Lista schemas disponíveis no Redshift"""
    sql = """
    SELECT nspname AS schema
    FROM pg_namespace
    WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'pg_internal')
    ORDER BY 1
    """
    with get_redshift_conn() as conn:
        df = pd.read_sql(sql, conn)
    return [str(x) for x in df["schema"].tolist()]


def get_tables(schema: str) -> list[str]:
    """Lista tabelas de um schema"""
    # Try multiple approaches to list tables
    queries = [
        # Approach 1: pg_table_def
        f"""
        SELECT DISTINCT tablename AS table
        FROM pg_table_def
        WHERE schemaname = '{schema}'
        ORDER BY 1
        """,
        # Approach 2: information_schema
        f"""
        SELECT table_name AS table
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        ORDER BY 1
        """,
        # Approach 3: pg_tables
        f"""
        SELECT tablename AS table
        FROM pg_tables
        WHERE schemaname = '{schema}'
        ORDER BY 1
        """
    ]
    
    for i, sql in enumerate(queries):
        try:
            with get_redshift_conn() as conn:
                df = pd.read_sql(sql, conn)
            tables = [str(x) for x in df["table"].tolist()]
            if tables:  # If we found tables, return them
                return tables
        except Exception as e:
            continue  # Try next approach
    
    return []  # No tables found with any approach


def get_columns(schema: str, table: str) -> pd.DataFrame:
    """Lista colunas de uma tabela"""
    sql = f"""
    SELECT "column", type
    FROM pg_table_def
    WHERE schemaname = '{schema}' AND tablename = '{table}'
    ORDER BY 1
    """
    with get_redshift_conn() as conn:
        return pd.read_sql(sql, conn)


def get_table_metrics(schema: str, table: str, ts_col: str | None) -> dict:
    """Obtém métricas de uma tabela"""
    parts = []
    parts.append(f"SELECT COUNT(*) AS row_count FROM {schema}.{table}")
    parts.append(f"SELECT SUM(rows) AS est_rows FROM svv_table_info WHERE schema = '{schema}' AND table = '{table}'")
    if ts_col:
        parts.append(f"SELECT MAX({ts_col}) AS max_ts FROM {schema}.{table}")
    
    metrics = {"row_count": None, "est_rows": None, "max_ts": None}
    
    with get_redshift_conn() as conn:
        try:
            df1 = pd.read_sql(parts[0], conn)
            metrics["row_count"] = int(df1.iloc[0, 0]) if not df1.empty else None
        except Exception:
            pass
        try:
            df2 = pd.read_sql(parts[1], conn)
            v = df2.iloc[0, 0] if not df2.empty else None
            metrics["est_rows"] = int(v) if v is not None else None
        except Exception:
            pass
        if ts_col:
            try:
                df3 = pd.read_sql(parts[2], conn)
                metrics["max_ts"] = pd.to_datetime(df3.iloc[0, 0], utc=True, errors="coerce") if not df3.empty else None
            except Exception:
                pass
    
    return metrics


def get_table_preview(schema: str, table: str, ts_col: str | None, limit: int = 20) -> pd.DataFrame:
    """Obtém preview de uma tabela"""
    order = f"ORDER BY {ts_col} DESC" if ts_col else ""
    sql = f"SELECT * FROM {schema}.{table} {order} LIMIT {limit}"
    with get_redshift_conn() as conn:
        return pd.read_sql(sql, conn)
