# -*- coding: utf-8 -*-
"""
Configurações e constantes do Monitor DW
"""

from datetime import datetime
import os

# Compatibilidade com versões mais antigas do Python
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

# ======================== CONFIGURAÇÕES GERAIS ========================
TZ = ZoneInfo("America/Sao_Paulo")
HISTORY_DB_PATH = "monitor_history.db"
USERS_DB_PATH = os.getenv("USERS_DB_PATH", ".users.json")

# ======================== THRESHOLDS E LIMITES ========================
REDSHIFT_THRESHOLD_MIN = 10     # queries > 10 min
REFRESH_ALERT_MIN = 180         # refresh > 180 min
AUTO_REFRESH_SEC = 60
KPI_ALERT_PCT = float(os.getenv("KPI_ALERT_PCT", "0.20"))

# ======================== CONSTANTES DE NEGÓCIO ========================
FIRST_ORDER_MAGENTO = "2023-06-20"

# ======================== CONFIGURAÇÕES DE CACHE ========================
CACHE_TTL_SHORT = 5    # 5 segundos para queries críticas
CACHE_TTL_MEDIUM = 60  # 1 minuto para dados menos críticos
CACHE_TTL_LONG = 300   # 5 minutos para dados estáticos

# ======================== CONFIGURAÇÕES DE UI ========================
PRIMARY = "#0EA5E9"   # azul
OK      = "#22C55E"   # verde
WARN    = "#F59E0B"   # amarelo
ERR     = "#EF4444"   # vermelho
MUTE    = "#6B7280"   # cinza

# ======================== HELPERS DE FORMATAÇÃO ========================
def _as_date_str_local(d: datetime) -> str:
    """Converte datetime para string de data local"""
    return d.astimezone(TZ).strftime("%Y-%m-%d")

def _as_minute_str_utc(d: datetime) -> str:
    """Converte datetime para string de minuto UTC"""
    return d.astimezone(datetime.now().astimezone().tzinfo).strftime("%Y-%m-%d %H:%M")

def _to_tz_aware_utc(x) -> datetime | None:
    """Converte para timestamp timezone-aware UTC"""
    if x is None:
        return None
    try:
        import pandas as pd
        ts = pd.to_datetime(x, errors="coerce", utc=True)
        if ts is None or pd.isna(ts):
            return None
        return ts
    except Exception:
        return None

def _fmt_sampa(ts: datetime | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Formata timestamp para timezone de São Paulo"""
    if ts is None:
        return "—"
    try:
        import pandas as pd
        if isinstance(ts, pd.Timestamp):
            return ts.tz_convert(TZ).strftime(fmt)
        else:
            return ts.astimezone(TZ).strftime(fmt)
    except Exception:
        return "—"

def _kfmt(v):
    """Formata números em formato k (milhares)"""
    return f"{v/1000:,.1f}k".replace(",", "X").replace(".", ",").replace("X", ".")

def _pct(v):
    """Formata números como porcentagem"""
    return f"{100*v:0.1f}%"

def get_now_kestra_style() -> datetime:
    """Obtém datetime atual com lógica do Kestra"""
    now = datetime.now(tz=TZ)
    if now.hour == 0 and now.minute < 30:
        now = now - datetime.timedelta(minutes=now.minute + 1)
    return now
