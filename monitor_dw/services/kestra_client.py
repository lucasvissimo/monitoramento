# -*- coding: utf-8 -*-
"""
Cliente para integração com Kestra API
"""

import requests
import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import base64


def get_kestra_auth_header() -> Dict[str, str]:
    """Gera header de autenticação API Key para Kestra"""
    try:
        api_key = st.secrets["kestra"]["api_key"]
        
        return {
            "X-EVINO-KESTRA-API-KEY": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "MonitorDW/1.0"
        }
    except Exception as e:
        st.error(f"Erro ao configurar autenticação Kestra: {e}")
        return {}


def get_kestra_base_url() -> str:
    """Obtém a URL base do Kestra"""
    try:
        return st.secrets["kestra"]["base_url"]
    except Exception:
        return "http://localhost:8080"  # URL padrão


def get_kestra_tenant() -> str:
    """Obtém o tenant do Kestra"""
    try:
        return st.secrets["kestra"].get("tenant", "default")
    except Exception:
        return "default"


@st.cache_data(ttl=30, show_spinner=False)
def get_kestra_flows() -> List[Dict]:
    """Obtém lista de flows do Kestra"""
    try:
        base_url = get_kestra_base_url()
        tenant = get_kestra_tenant()
        headers = get_kestra_auth_header()
        
        if not headers:
            return []
        
        # URL baseada na documentação oficial: /api/v1/{tenant}/flows
        url = f"{base_url}/api/v1/{tenant}/flows"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                # Verificar se a resposta é JSON
                content_type = response.headers.get('content-type', '').lower()
                if 'application/json' in content_type:
                    try:
                        data = response.json()
                        # Diferentes estruturas de resposta
                        if isinstance(data, list):
                            return data
                        elif isinstance(data, dict):
                            return data.get("results", data.get("flows", []))
                        return []
                    except json.JSONDecodeError:
                        st.error(f"Erro ao decodificar JSON da resposta")
                        return []
                else:
                    st.error(f"Resposta não é JSON: {response.text[:200]}")
                    return []
            else:
                st.error(f"Erro na requisição: {response.status_code} - {response.text[:200]}")
                return []
                
        except Exception as e:
            st.error(f"Erro na requisição para Kestra: {e}")
            return []
            
    except Exception as e:
        st.error(f"Erro na requisição para Kestra: {e}")
        return []


@st.cache_data(ttl=10, show_spinner=False)
def get_kestra_executions(flow_id: str, limit: int = 10) -> List[Dict]:
    """Obtém execuções de um flow específico"""
    try:
        base_url = get_kestra_base_url()
        tenant = get_kestra_tenant()
        headers = get_kestra_auth_header()
        
        if not headers:
            return []
        
        # URL baseada na documentação oficial: /api/v1/{tenant}/executions/flows/{namespace}/{flowId}
        url = f"{base_url}/api/v1/{tenant}/executions/flows/main/{flow_id}"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                if 'application/json' in content_type:
                    try:
                        data = response.json()
                        if isinstance(data, list):
                            return data
                        elif isinstance(data, dict):
                            return data.get("results", data.get("executions", []))
                        return []
                    except json.JSONDecodeError:
                        st.error(f"Erro ao decodificar JSON da resposta")
                        return []
                else:
                    st.error(f"Resposta não é JSON: {response.text[:200]}")
                    return []
            else:
                st.error(f"Erro na requisição: {response.status_code} - {response.text[:200]}")
                return []
                
        except Exception as e:
            st.error(f"Erro na requisição para Kestra: {e}")
            return []
            
    except Exception as e:
        st.error(f"Erro na requisição para Kestra: {e}")
        return []


@st.cache_data(ttl=5, show_spinner=False)
def get_kestra_execution_status(execution_id: str) -> Optional[Dict]:
    """Obtém status detalhado de uma execução específica"""
    try:
        base_url = get_kestra_base_url()
        tenant = get_kestra_tenant()
        headers = get_kestra_auth_header()
        
        if not headers:
            return None
        
        # URL baseada na documentação oficial: /api/v1/{tenant}/executions/{executionId}
        url = f"{base_url}/api/v1/{tenant}/executions/{execution_id}"
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Erro ao obter status da execução {execution_id}: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        st.error(f"Erro na requisição para Kestra: {e}")
        return None


def get_flow_status_from_docs(flow_id: str, namespace: str = "main") -> Dict:
    """Obtém status do flow usando endpoint da documentação oficial"""
    try:
        base_url = get_kestra_base_url()
        tenant = get_kestra_tenant()
        headers = get_kestra_auth_header()
        
        if not headers:
            return {"status": "ERROR", "message": "Headers não configurados"}
        
        # URL baseada na documentação: GET /api/v1/{tenant}/executions/flows/{namespace}/{flowId}
        url = f"{base_url}/api/v1/{tenant}/executions/flows/{namespace}/{flow_id}"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                if 'application/json' in content_type:
                    try:
                        data = response.json()
                        
                        # Processar resposta para extrair status
                        if isinstance(data, list) and data:
                            # Pegar a execução mais recente
                            latest_execution = data[0]
                            return {
                                "status": "SUCCESS",
                                "flow_id": flow_id,
                                "namespace": namespace,
                                "latest_execution": latest_execution,
                                "execution_count": len(data),
                                "last_run": latest_execution.get("createdDate"),
                                "state": latest_execution.get("state", "UNKNOWN")
                            }
                        elif isinstance(data, dict):
                            return {
                                "status": "SUCCESS",
                                "flow_id": flow_id,
                                "namespace": namespace,
                                "data": data,
                                "message": "Dados recebidos com sucesso"
                            }
                        else:
                            return {
                                "status": "NO_EXECUTIONS",
                                "flow_id": flow_id,
                                "namespace": namespace,
                                "message": "Nenhuma execução encontrada"
                            }
                            
                    except json.JSONDecodeError:
                        return {
                            "status": "ERROR",
                            "message": f"Erro ao decodificar JSON: {response.text[:200]}"
                        }
                else:
                    return {
                        "status": "ERROR",
                        "message": f"Resposta não é JSON: {response.text[:200]}"
                    }
            else:
                return {
                    "status": "ERROR",
                    "message": f"Erro HTTP {response.status_code}: {response.text[:200]}"
                }
                
        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Erro na requisição: {str(e)}"
            }
            
    except Exception as e:
        return {
            "status": "ERROR",
            "message": f"Erro geral: {str(e)}"
        }


def get_flow_last_execution_status(flow_id: str) -> Dict:
    """Obtém o status da última execução de um flow"""
    try:
        executions = get_kestra_executions(flow_id, limit=1)
        
        if not executions:
            return {
                "status": "NO_EXECUTIONS",
                "message": "Nenhuma execução encontrada",
                "last_run": None,
                "duration": None
            }
        
        last_execution = executions[0]
        execution_id = last_execution.get("id")
        
        # Obter detalhes da execução
        execution_details = get_kestra_execution_status(execution_id)
        
        if not execution_details:
            return {
                "status": "ERROR",
                "message": "Erro ao obter detalhes da execução",
                "last_run": None,
                "duration": None
            }
        
        status = execution_details.get("state", {}).get("current", "UNKNOWN")
        start_date = execution_details.get("state", {}).get("startDate")
        end_date = execution_details.get("state", {}).get("endDate")
        
        # Calcular duração
        duration = None
        if start_date and end_date:
            try:
                start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                duration = (end - start).total_seconds()
            except Exception:
                pass
        
        # Determinar status e mensagem
        if status == "SUCCESS":
            message = "Execução bem-sucedida"
        elif status == "FAILED":
            message = "Execução falhou"
        elif status == "RUNNING":
            message = "Execução em andamento"
        elif status == "KILLED":
            message = "Execução cancelada"
        else:
            message = f"Status: {status}"
        
        return {
            "status": status,
            "message": message,
            "last_run": start_date,
            "duration": duration,
            "execution_id": execution_id,
            "flow_id": flow_id
        }
        
    except Exception as e:
        return {
            "status": "ERROR",
            "message": f"Erro ao obter status: {str(e)}",
            "last_run": None,
            "duration": None
        }


def get_multiple_flows_status(flow_ids: List[str]) -> Dict[str, Dict]:
    """Obtém status de múltiplos flows"""
    results = {}
    
    for flow_id in flow_ids:
        results[flow_id] = get_flow_last_execution_status(flow_id)
    
    return results


def trigger_kestra_flow(flow_id: str, inputs: Dict = None) -> Dict:
    """Dispara uma execução de um flow no Kestra"""
    try:
        base_url = get_kestra_base_url()
        tenant = get_kestra_tenant()
        headers = get_kestra_auth_header()
        
        if not headers:
            return {"success": False, "message": "Erro de autenticação"}
        
        # URL original fornecida pelo usuário
        url = f"{base_url}/api/v1/main/executions"
        payload = {
            "flowId": flow_id
        }
        
        if inputs:
            payload["inputs"] = inputs
        
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        
        if response.status_code in [200, 201]:
            return {
                "success": True,
                "message": "Flow disparado com sucesso",
                "execution_id": response.json().get("id")
            }
        else:
            return {
                "success": False,
                "message": f"Erro ao disparar flow: {response.status_code} - {response.text}"
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": f"Erro na requisição: {str(e)}"
        }
