# -*- coding: utf-8 -*-
"""
Serviço de alertas via Slack
"""

import json
import time
import requests
import streamlit as st
from ..config import TZ
from datetime import datetime


def slack_post(text: str = "", blocks: list | None = None, webhook_url: str | None = None, timeout: int = 15):
    """
    Envia mensagem para Slack via webhook
    Retorna: (success: bool, message: str)
    """
    try:
        WEBHOOK_URL = st.secrets["slack"]["webhook_url"].strip()
    except Exception:
        WEBHOOK_URL = ""
    
    url = (webhook_url or WEBHOOK_URL or "").strip()
    if not url or "hooks.slack.com" not in url:
        return False, "WEBHOOK_URL não configurado"

    # Validate URL format
    if not url.startswith("https://hooks.slack.com/services/"):
        return False, f"URL do webhook parece inválida. Esperado: https://hooks.slack.com/services/... Recebido: {url[:50]}..."

    payload = {"text": text or ""}
    if blocks:
        payload["blocks"] = blocks

    backoff = 1.0
    last_err = "desconhecido"
    for _ in range(4):
        try:
            resp = requests.post(url, headers={"Content-type": "application/json"}, data=json.dumps(payload), timeout=timeout)
            if resp.status_code in (200, 204):
                return True, "ok"
            if resp.status_code == 404:
                # Webhook inválido/revogado ou endpoint não encontrado
                body = resp.text[:200]
                if "no_service" in body.lower():
                    return False, f"🚨 Webhook Slack foi REVOGADO ou não existe mais (404 no_service). Você precisa criar um novo webhook no Slack. Resposta: {body}"
                else:
                    return False, f"Endpoint não encontrado (404). URL pode estar incorreta. Resposta: {body}"
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
        except requests.exceptions.Timeout:
            return False, f"Timeout ao conectar com Slack (>{timeout}s)"
        except requests.exceptions.ConnectionError:
            return False, "Erro de conexão com Slack. Verifique sua internet."
        except Exception as e:
            return False, f"Erro inesperado: {str(e)}"
    return False, last_err


def _fmt_sql_list(df_list, max_items: int = 5) -> str:
    """Formata lista de queries SQL para Slack"""
    if df_list is None or df_list.empty:
        return "—"
    linhas = []
    for _, r in df_list.head(max_items).iterrows():
        pid = r.get("pid", r.get("PID", "—"))
        user = r.get("user_name", r.get("USER_NAME", "—"))
        durm = r.get("duration_minutes", r.get("DURATION_MINUTES", None))
        dur_str = f"{float(durm):.1f}m" if durm is not None else "—"
        q = str(r.get("query", r.get("QUERY", ""))).replace("", " ").strip()[:120]
        linhas.append(f"• `pid:{pid}` ({user}, {dur_str}) — {q}")
    return "".join(linhas) if linhas else "—"


def _fmt_pb_utc(ts_utc):
    """Formata timestamp UTC do Power BI para Slack"""
    if ts_utc is None:
        return "—"
    try:
        import pandas as pd
        return pd.to_datetime(ts_utc, utc=True).strftime('%d/%m/%Y %H:%M:%S UTC')
    except Exception:
        return "—"


def create_alert_blocks(query_bad: bool, powerbi_bad: bool, jira_bad: bool, 
                       running_over: int, last_refresh_utc, jira_total: int, 
                       redshift_threshold: int) -> list[dict]:
    """Cria blocos de alerta para Slack"""
    now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "🚨 Monitor DW — Errors"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"⏱️ {now_str} • Enviado automaticamente"}]},
        {"type": "divider"},
    ]
    
    if query_bad:
        blocks.append({
            "type": "section", 
            "text": {"type": "mrkdwn", "text": f"*Queries acima de {redshift_threshold} min:* {running_over}"}
        })
    
    if powerbi_bad:
        blocks.append({
            "type": "section", 
            "text": {"type": "mrkdwn", "text": f"*Power BI — Último refresh (UTC):* {_fmt_pb_utc(last_refresh_utc)}"}
        })
    
    if jira_bad:
        blocks.append({
            "type": "section", 
            "text": {"type": "mrkdwn", "text": f"*Chamados abertos (Jira):* {jira_total}"}
        })
    
    return blocks


def send_alert_if_needed(query_bad: bool, powerbi_bad: bool, jira_bad: bool, 
                        running_over: int, last_refresh_utc, jira_total: int, 
                        redshift_threshold: int, age_min: int = None) -> tuple[bool, str]:
    """
    Envia alerta para Slack se necessário (com deduplicação)
    Retorna: (sent: bool, message: str)
    """
    any_bad = query_bad or powerbi_bad or jira_bad
    
    if not any_bad:
        return False, "✅ Sem erros; nenhum alerta enviado ao Slack."
    
    # De-dup e envio
    digest_data = {
        "q": running_over if query_bad else 0,
        "pb_age": age_min if powerbi_bad else 0,
        "jira": jira_total if jira_bad else 0,
        "t": datetime.now(TZ).replace(minute=(datetime.now(TZ).minute // 15) * 15, second=0, microsecond=0).isoformat(),
    }
    digest = json.dumps(digest_data, ensure_ascii=False, sort_keys=True)
    
    # Check if we already sent this alert recently
    if "last_alert_digest" not in st.session_state or st.session_state.get("last_alert_digest") != digest:
        blocks = create_alert_blocks(query_bad, powerbi_bad, jira_bad, running_over, last_refresh_utc, jira_total, redshift_threshold)
        ok, info = slack_post(text="🚨 Monitor DW — errors", blocks=blocks)
        
        st.session_state["last_alert_digest"] = digest
        if not ok and ("404" in info or "Webhook inválido" in info):
            st.session_state["disable_slack_alerts"] = True
        
        return True, f"🔔 Alerta Slack: {'ok' if ok else 'falhou'} — {info}"
    else:
        return False, "🔕 Sem mudanças relevantes nas erros; nenhum novo alerta enviado."


def test_slack_webhook(test_message: str = "Teste do Monitor DW ✔️", webhook_override: str = "") -> tuple[bool, str]:
    """
    Testa webhook do Slack
    Retorna: (success: bool, message: str)
    """
    try:
        WEBHOOK_URL = st.secrets["slack"]["webhook_url"].strip()
    except Exception:
        WEBHOOK_URL = ""
    
    TARGET_WEBHOOK = (webhook_override or WEBHOOK_URL or "").strip()
    if not TARGET_WEBHOOK or "hooks.slack.com" not in TARGET_WEBHOOK:
        return False, "Webhook ausente ou inválido (esperado hooks.slack.com)"
    
    ok, info = slack_post(test_message, webhook_url=TARGET_WEBHOOK)
    
    # Store result for UI feedback
    st.session_state["slack_test_result"] = {
        "ts": time.time(),
        "ok": ok,
        "info": info
    }
    
    # If invalid webhook, disable auto alerts until fixed
    if not ok and "Webhook inválido" in info:
        st.session_state["disable_slack_alerts"] = True
    
    # Pause auto refresh for 8s so the user sees the feedback
    st.session_state["suspend_auto_refresh_until"] = time.time() + 8
    
    return ok, info
