# -*- coding: utf-8 -*-
"""
Conexões e executores de banco de dados
"""

import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from .config import HISTORY_DB_PATH, TZ

# psycopg2 will be imported only when needed
PSYCOPG2_AVAILABLE = None
psycopg2 = None

def _get_psycopg2():
    """Import psycopg2 only when needed"""
    global PSYCOPG2_AVAILABLE, psycopg2
    if PSYCOPG2_AVAILABLE is None:
        try:
            import psycopg2
            PSYCOPG2_AVAILABLE = True
        except ImportError:
            try:
                # Try importing psycopg2-binary as fallback
                import psycopg2
                PSYCOPG2_AVAILABLE = True
            except ImportError:
                PSYCOPG2_AVAILABLE = False
                psycopg2 = None
                print("⚠️ psycopg2 não disponível. Funcionalidades de banco de dados limitadas.")
    return psycopg2, PSYCOPG2_AVAILABLE


# ======================== HISTORY DATABASE ========================
def init_history_db():
    """Inicializa o banco de dados de histórico"""
    conn = sqlite3.connect(HISTORY_DB_PATH)
    conn.execute("PRAGMA encoding = 'UTF-8'")
    cursor = conn.cursor()
    
    # User logins table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ip_address TEXT,
            user_agent TEXT
        )
    """)
    
    # Error counts table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS error_counts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            error_type TEXT NOT NULL,
            error_count INTEGER DEFAULT 1,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            details TEXT
        )
    """)
    
    # Daily summaries table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            redshift_queries_over_10min INTEGER DEFAULT 0,
            jira_tickets_opened INTEGER DEFAULT 0,
            powerbi_refresh_delays INTEGER DEFAULT 0,
            kpi_anomalies INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()


def log_user_login(username: str):
    """Registra login do usuário"""
    conn = sqlite3.connect(HISTORY_DB_PATH)
    conn.execute("PRAGMA encoding = 'UTF-8'")
    cursor = conn.cursor()
    # Use current timestamp with timezone for accurate time tracking
    now_utc = datetime.now(timezone.utc)
    cursor.execute("""
        INSERT INTO user_logins (username, login_time)
        VALUES (?, ?)
    """, (username, now_utc.isoformat()))
    conn.commit()
    conn.close()


def log_error(error_type: str, details: str = ""):
    """Registra erro no histórico"""
    try:
        conn = sqlite3.connect(HISTORY_DB_PATH)
        conn.execute("PRAGMA encoding = 'UTF-8'")
        cursor = conn.cursor()
        
        # Check if we already logged this error type recently (within last 30 minutes)
        cursor.execute("""
            SELECT COUNT(*) FROM error_counts 
            WHERE error_type = ? 
            AND timestamp >= datetime('now', '-30 minutes')
        """, (error_type,))
        
        recent_count = cursor.fetchone()[0]
        
        # Only log if we haven't logged this error type recently
        if recent_count == 0:
            cursor.execute("""
                INSERT INTO error_counts (error_type, details, timestamp)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (error_type, details))
            conn.commit()
            print(f"✅ Erro logado: {error_type} - {details}")
        else:
            print(f"⏭️ Erro já logado recentemente: {error_type}")
        
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao logar erro: {str(e)}")


def get_error_stats(days: int = 7) -> dict:
    """Obtém estatísticas de erros dos últimos N dias"""
    conn = sqlite3.connect(HISTORY_DB_PATH)
    conn.execute("PRAGMA encoding = 'UTF-8'")
    cursor = conn.cursor()
    
    # Get error counts for last N days
    cursor.execute("""
        SELECT error_type, COUNT(*) as count
        FROM error_counts 
        WHERE timestamp >= datetime('now', '-{} days')
        GROUP BY error_type
    """.format(days))
    
    error_stats = dict(cursor.fetchall())
    
    # Get daily summaries
    cursor.execute("""
        SELECT date, redshift_queries_over_10min, jira_tickets_opened, powerbi_refresh_delays, kpi_anomalies
        FROM daily_summaries 
        WHERE date >= date('now', '-{} days')
        ORDER BY date DESC
    """.format(days))
    
    daily_stats = cursor.fetchall()
    
    conn.close()
    
    return {
        "error_counts": error_stats,
        "daily_summaries": daily_stats
    }


def cleanup_duplicate_errors():
    """Remove entradas duplicadas de erros"""
    conn = sqlite3.connect(HISTORY_DB_PATH)
    conn.execute("PRAGMA encoding = 'UTF-8'")
    cursor = conn.cursor()
    
    # Remove duplicate jira_tickets_opened entries (keep only 1 per day)
    cursor.execute("""
        DELETE FROM error_counts 
        WHERE error_type = 'jira_tickets_opened' 
        AND id NOT IN (
            SELECT MIN(id) 
            FROM error_counts 
            WHERE error_type = 'jira_tickets_opened'
            GROUP BY DATE(timestamp)
        )
    """)
    
    # Remove excessive powerbi_refresh_delay entries (keep only 1 per hour)
    cursor.execute("""
        DELETE FROM error_counts 
        WHERE error_type = 'powerbi_refresh_delay' 
        AND id NOT IN (
            SELECT MIN(id) 
            FROM error_counts 
            WHERE error_type = 'powerbi_refresh_delay'
            GROUP BY DATE(timestamp), strftime('%H', timestamp)
        )
    """)
    
    conn.commit()
    conn.close()


def update_daily_summary(date_str: str, redshift_queries: int = 0, jira_tickets: int = 0, 
                        powerbi_delays: int = 0, kpi_anomalies: int = 0):
    """Atualiza resumo diário"""
    conn = sqlite3.connect(HISTORY_DB_PATH)
    cursor = conn.cursor()
    
    # Check if summary exists for this date
    cursor.execute("SELECT id FROM daily_summaries WHERE date = ?", (date_str,))
    existing = cursor.fetchone()
    
    if existing:
        # Update existing
        cursor.execute("""
            UPDATE daily_summaries 
            SET redshift_queries_over_10min = redshift_queries_over_10min + ?,
                jira_tickets_opened = jira_tickets_opened + ?,
                powerbi_refresh_delays = powerbi_refresh_delays + ?,
                kpi_anomalies = kpi_anomalies + ?
            WHERE date = ?
        """, (redshift_queries, jira_tickets, powerbi_delays, kpi_anomalies, date_str))
    else:
        # Insert new
        cursor.execute("""
            INSERT INTO daily_summaries (date, redshift_queries_over_10min, jira_tickets_opened, powerbi_refresh_delays, kpi_anomalies)
            VALUES (?, ?, ?, ?, ?)
        """, (date_str, redshift_queries, jira_tickets, powerbi_delays, kpi_anomalies))
    
    conn.commit()
    conn.close()


# ======================== CONEXÕES DB ========================
@st.cache_resource(show_spinner=False)
def get_redshift_conn():
    """Obtém conexão com Redshift com configurações robustas"""
    psycopg2, available = _get_psycopg2()
    if not available:
        raise ImportError("psycopg2 não está disponível. Instale com: pip install psycopg2-binary")
    
    s = st.secrets["dw_vissimo"]
    
    try:
        conn = psycopg2.connect(
            host=s["host"],
            port=s.get("port", 5439),
            dbname=s["dbname"],
            user=s["user"],
            password=s["password"],
            connect_timeout=10,
            application_name="MonitorDW",
            keepalives_idle=600,
            keepalives_interval=30,
            keepalives_count=3
        )
        conn.autocommit = True
        
        # Configurar encoding
        try:
            conn.set_client_encoding("UTF8")
        except Exception:
            pass
            
        # Testar a conexão
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
            
        print("✅ Conexão Redshift estabelecida com sucesso")
        return conn
        
    except Exception as e:
        print(f"❌ Erro ao conectar com Redshift: {str(e)}")
        raise


@st.cache_resource(show_spinner=False)
def get_postgres_conn():
    """Obtém conexão com Postgres com configurações robustas"""
    psycopg2, available = _get_psycopg2()
    if not available:
        raise ImportError("psycopg2 não está disponível. Instale com: pip install psycopg2-binary")
    
    s = st.secrets["postgres"]
    
    try:
        conn = psycopg2.connect(
            host=s["host"],
            port=s.get("port", 5432),
            dbname=s["dbname"],
            user=s["user"],
            password=s["password"],
            connect_timeout=10,
            application_name="MonitorDW",
            keepalives_idle=600,
            keepalives_interval=30,
            keepalives_count=3
        )
        conn.autocommit = True
        
        # Configurar encoding
        try:
            conn.set_client_encoding("UTF8")
        except Exception:
            pass
            
        # Testar a conexão
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
            
        print("✅ Conexão PostgreSQL estabelecida com sucesso")
        return conn
        
    except Exception as e:
        print(f"❌ Erro ao conectar com PostgreSQL: {str(e)}")
        raise


# ======================== EXECUTORES DE QUERY ========================
@st.cache_data(ttl=5, show_spinner=False)
def run_redshift(sql: str) -> pd.DataFrame:
    """Executa query no Redshift com retry automático"""
    psycopg2, available = _get_psycopg2()
    if not available:
        st.warning("⚠️ psycopg2 não disponível. Funcionalidades de Redshift desabilitadas.")
        return pd.DataFrame()
    
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            conn = get_redshift_conn()
            
            # Verificar se a conexão está ativa
            if conn.closed:
                print(f"⚠️ Conexão Redshift fechada, tentando reconectar... (tentativa {attempt + 1})")
                # Limpar cache da conexão para forçar nova conexão
                get_redshift_conn.clear()
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
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Erro na consulta Redshift (tentativa {attempt + 1}): {error_msg}")
            
            if attempt < max_retries - 1:
                print(f"🔄 Tentando novamente em {retry_delay} segundos...")
                import time
                time.sleep(retry_delay)
                retry_delay *= 2  # Backoff exponencial
                
                # Limpar cache da conexão para forçar nova conexão
                get_redshift_conn.clear()
            else:
                st.error(f"❌ Erro na consulta Redshift após {max_retries} tentativas: {error_msg}")
                return pd.DataFrame()


@st.cache_data(ttl=5, show_spinner=False)
def run_postgres(sql: str) -> pd.DataFrame:
    """Executa query no Postgres com retry automático"""
    psycopg2, available = _get_psycopg2()
    if not available:
        st.warning("⚠️ psycopg2 não disponível. Funcionalidades de PostgreSQL desabilitadas.")
        return pd.DataFrame()
    
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            conn = get_postgres_conn()
            
            # Verificar se a conexão está ativa
            if conn.closed:
                print(f"⚠️ Conexão PostgreSQL fechada, tentando reconectar... (tentativa {attempt + 1})")
                # Limpar cache da conexão para forçar nova conexão
                get_postgres_conn.clear()
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
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Erro na consulta Postgres (tentativa {attempt + 1}): {error_msg}")
            
            if attempt < max_retries - 1:
                print(f"🔄 Tentando novamente em {retry_delay} segundos...")
                import time
                time.sleep(retry_delay)
                retry_delay *= 2  # Backoff exponencial
                
                # Limpar cache da conexão para forçar nova conexão
                get_postgres_conn.clear()
            else:
                st.error(f"❌ Erro na consulta Postgres após {max_retries} tentativas: {error_msg}")
                return pd.DataFrame()


def clear_all_db_connections():
    """Limpa todas as conexões de banco de dados em cache"""
    try:
        get_redshift_conn.clear()
        get_postgres_conn.clear()
        print("✅ Todas as conexões de banco de dados foram limpas")
        return True
    except Exception as e:
        print(f"❌ Erro ao limpar conexões: {e}")
        return False


def log_jira_tickets(ticket_count: int):
    """Registra chamados abertos do Jira no histórico"""
    try:
        from datetime import datetime
        from .config import TZ
        today_str = datetime.now(TZ).strftime("%Y-%m-%d")
        
        # Atualizar resumo diário
        update_daily_summary(today_str, jira_tickets=ticket_count)
        
        # Registrar no log de erros (para histórico)
        log_error("jira_tickets_opened", f"Chamados abertos: {ticket_count}")
        
        print(f"✅ {ticket_count} chamados do Jira registrados no histórico")
        return True
    except Exception as e:
        print(f"❌ Erro ao registrar chamados do Jira: {e}")
        return False
