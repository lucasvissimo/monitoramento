# -*- coding: utf-8 -*-
"""
Cliente para consultas do Jira
"""

import requests
import streamlit as st
from ..config import TZ
from datetime import datetime


@st.cache_data(ttl=60, show_spinner=False)
def jira_approx_count(jql: str) -> int:
    """Obtém contagem aproximada de issues do Jira"""
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
    """Busca issues do Jira"""
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


def get_open_tickets() -> tuple[int, list[dict]]:
    """
    Obtém tickets abertos do projeto TD
    Retorna: (total_count, issues_list)
    """
    jql_td_open = (
        "project = TD AND resolution IS EMPTY AND statusCategory IN ('To Do','In Progress') "
        "AND (assignee = currentUser() OR assignee IS EMPTY)"
    )
    
    try:
        total_abertos = jira_approx_count(jql_td_open)
        issues = jira_fetch_issues(jql_td_open, max_results=20)
        return total_abertos, issues
    except Exception as e:
        st.error(f"Falha ao consultar Jira: {e}")
        return 0, []


def has_jira_anomaly(jira_total: int) -> bool:
    """Verifica se há anomalia no Jira (tickets abertos)"""
    return jira_total > 0


def format_issues_for_display(issues: list[dict]) -> list[dict]:
    """Formata issues para exibição"""
    base_url = st.secrets["jira"]["base_url"].rstrip("/")
    rows = []
    
    for it in issues:
        f = it.get("fields", {})
        
        # Extrair dados com tratamento de erro
        chamado = it.get("key", "N/A")
        resumo = str(f.get("summary", "")).strip() if f.get("summary") else "N/A"
        status_obj = f.get("status", {})
        status = status_obj.get("name", "N/A") if status_obj else "N/A"
        assignee_obj = f.get("assignee", {})
        responsavel = assignee_obj.get("displayName", "—") if assignee_obj else "—"
        atualizado = f.get("updated", "N/A")
        
        rows.append({
            "Chamado": chamado,
            "Resumo": resumo,
            "Status": status,
            "Responsável": responsavel,
            "Atualizado": atualizado,
            "URL": f"{base_url}/browse/{chamado}",
        })
    
    return rows


def clear_jira_cache():
    """Limpa o cache do Jira"""
    try:
        jira_approx_count.clear()
        jira_fetch_issues.clear()
        print("✅ Cache do Jira limpo com sucesso")
        return True
    except Exception as e:
        print(f"❌ Erro ao limpar cache do Jira: {e}")
        return False