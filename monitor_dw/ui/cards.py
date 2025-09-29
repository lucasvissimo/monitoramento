# -*- coding: utf-8 -*-
"""
Componentes de cards e renderização de dados
"""

import streamlit as st
import pandas as pd
from ..config import TZ, _kfmt, _pct, _fmt_sampa
from datetime import datetime


def render_overview_card(running_over: int, last_refresh_utc, powerbi_bad: bool, 
                        total_abertos: int, today_revenue: float, today_forecast: float, 
                        redshift_threshold: int, mons_count: int, stale_count: int = 0):
    """Renderiza card de visão geral"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("<h3>Resumo em tempo real</h3>", unsafe_allow_html=True)
    
    # Indicador de performance
    st.caption(f"🕐 Última atualização: {datetime.now(TZ).strftime('%H:%M:%S')}")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"<div class='metric'><div class='label'>Queries > {int(redshift_threshold)} min</div><div class='value'>{running_over}</div></div>", unsafe_allow_html=True)
    with c2:
        ts = "—" if last_refresh_utc is None else last_refresh_utc.strftime("%d/%m %H:%M")
        sit = "—" if last_refresh_utc is None else ("⚠️" if powerbi_bad else "✅")
        st.markdown(f"<div class='metric'><div class='label'>Power BI (UTC)</div><div class='value'>{ts}</div><div class='delta'>{sit}</div></div>", unsafe_allow_html=True)
    with c3:
        st.markdown(f"<div class='metric'><div class='label'>Jira abertos</div><div class='value'>{total_abertos}</div></div>", unsafe_allow_html=True)
    with c4:
        st.markdown(f"<div class='metric'><div class='label'>Receita hoje</div><div class='value'>{_kfmt(today_revenue)}</div><div class='delta'>Meta: {_kfmt(today_forecast)}</div></div>", unsafe_allow_html=True)

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        st.markdown(f"<div class='metric'><div class='label'>Monitores</div><div class='value'>{mons_count}</div><div class='delta'>{'⚠️ ' + str(stale_count) + ' com atraso' if stale_count else 'OK'}</div></div>", unsafe_allow_html=True)

    st.divider()
    st.markdown('</div>', unsafe_allow_html=True)


def render_redshift_card(running_over: int, redshift_threshold: int, df_list: pd.DataFrame = None):
    """Renderiza card do Redshift"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("<h3>Redshift — Queries Engasgadas</h3>", unsafe_allow_html=True)
    
    # Filtros avançados
    with st.expander("🔧 Filtros Avançados", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            min_duration = st.number_input("Duração mínima (min)", min_value=1, max_value=240, value=redshift_threshold, key="redshift_min_duration")
        with col2:
            TOP_N = st.number_input("Máx. resultados", min_value=5, max_value=100, value=20, key="redshift_max_results")
        with col3:
            show_all = st.checkbox("Mostrar todas as queries", help="Inclui queries dentro do limite", key="redshift_show_all")

    cols = st.columns(3)
    with cols[0]:
        st.markdown(
            f"""
            <div class="metric">
              <div class="label">Queries rodando demais</div>
              <div class="value">{running_over}</div>
              <div class="delta">{'&gt; ' + str(int(redshift_threshold)) + ' min' if running_over else 'OK'}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with cols[1]:
        st.markdown(
            f"<div class='metric'><div class='label'>Limite atual</div><div class='value'>{int(redshift_threshold)} min</div></div>",
            unsafe_allow_html=True,
        )
    with cols[2]:
        now_local = datetime.now(TZ).strftime('%d/%m %H:%M')
        st.markdown(
            f"<div class='metric'><div class='label'>Atualizado</div><div class='value'>{now_local}</div></div>",
            unsafe_allow_html=True,
        )

    if running_over > 0:
        st.error("⚠️ Existem queries em execução acima do limite configurado.")
    else:
        st.success("✅ Nenhuma query acima do limite no momento.")

    if running_over > 0 and df_list is not None and not df_list.empty:
        with st.expander("Ver queries (Top)", expanded=True):
            st.dataframe(df_list, use_container_width=True, height=360, hide_index=True)

    st.markdown('</div>', unsafe_allow_html=True)


def render_powerbi_card(refresh_info: dict, refresh_alert_min: int):
    """Renderiza card do Power BI"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("<h3>Power BI — Último refresh (Painel CD)</h3>", unsafe_allow_html=True)
    
    # Controles de atualização
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
        st.caption("💡 Dica: Use os botões abaixo para forçar atualização dos dados")
    with col2:
        if st.button("🔄 Atualizar", help="Limpa o cache e força uma nova consulta"):
            from ..db import run_postgres
            run_postgres.clear()
            st.success("✅ Cache limpo!")
            st.rerun()
    with col3:
        if st.button("📊 Status", help="Mostra informações de cache e conexão"):
            st.info(f"Cache TTL: 5s | Conexões ativas: Postgres")
    with col4:
        if st.button("🧹 Limpar Cache", help="Limpa todo o cache do Streamlit"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("✅ Todo cache limpo!")
            st.rerun()

    if refresh_info["status"] == "error":
        st.warning(refresh_info["message"])
    else:
        cols = st.columns(3)
        with cols[0]:
            last_refresh_utc = refresh_info["last_refresh_utc"]
            last_refresh_local = refresh_info["last_refresh_local"]
            age_min = refresh_info["age_minutes"]
            st.markdown(
                f"<div class='metric'><div class='label'>Último refresh — backlog_sap</div><div class='value'>{last_refresh_utc.strftime('%d/%m/%Y %H:%M:%S')} UTC</div><div class='delta'>{last_refresh_local.strftime('%H:%M:%S')} Local • {age_min:.0f} min atrás</div></div>",
                unsafe_allow_html=True,
            )
        with cols[1]:
            st.markdown(
                f"<div class='metric'><div class='label'>Limiar de alerta</div><div class='value'>{int(refresh_alert_min)} min</div></div>",
                unsafe_allow_html=True,
            )
        with cols[2]:
            st.markdown(
                f"<div class='metric'><div class='label'>Situação</div><div class='value'>{'⚠️ Atrasado' if refresh_info['is_anomaly'] else '✅ No prazo'}</div></div>",
                unsafe_allow_html=True,
            )

        if refresh_info["is_anomaly"]:
            st.error(refresh_info["message"])
            st.caption(f"🔧 Debug: Refresh deve estar no mesmo dia (UTC). Diferença: {refresh_info['age_minutes']:.0f} min")
        else:
            st.success(refresh_info["message"])
            st.caption(f"🔧 Debug: Refresh está no mesmo dia (UTC). Diferença: {refresh_info['age_minutes']:.0f} min")

    st.markdown('</div>', unsafe_allow_html=True)


def render_jira_card(total_abertos: int, issues: list[dict]):
    """Renderiza card do Jira"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("<h3>Jira — Chamados abertos</h3>", unsafe_allow_html=True)
    
    # Controles de atualização
    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption("💡 Dica: Clique em 'Atualizar' para buscar os chamados mais recentes")
    with col2:
        if st.button("🔄 Atualizar Jira", help="Força atualização dos dados do Jira"):
            from monitor_dw.services.jira_client import clear_jira_cache
            clear_jira_cache()
            st.cache_data.clear()
            st.success("✅ Cache limpo!")
            st.rerun()

    cols = st.columns(3)
    with cols[0]:
        st.markdown(
            f"<div class='metric'><div class='label'>Chamados abertos</div><div class='value'>{total_abertos}</div></div>",
            unsafe_allow_html=True,
        )
    with cols[1]:
        st.markdown(
            f"<div class='metric'><div class='label'>Filtro</div><div class='value'>TD • To Do / In Progress</div></div>",
            unsafe_allow_html=True,
        )
    with cols[2]:
        st.markdown(
            f"<div class='metric'><div class='label'>Atualizado</div><div class='value'>{datetime.now(TZ).strftime('%d/%m %H:%M')}</div></div>",
            unsafe_allow_html=True,
        )

    if issues:
        st.caption(f"📋 {len(issues)} chamados encontrados")
        
        # Os dados já vêm formatados da função format_issues_for_display
        df = pd.DataFrame(issues)
        
        # Converter data se necessário
        if "Atualizado" in df.columns:
            df["Atualizado"] = pd.to_datetime(df["Atualizado"], utc=True, errors="coerce").dt.tz_convert(TZ).dt.strftime("%d/%m %H:%M")
        
        try:
            st.dataframe(
                df[["Chamado", "Resumo", "Status", "Responsável", "Atualizado", "URL"]],
                use_container_width=True,
                height=380,
                hide_index=True,
                column_config={
                    "URL": st.column_config.LinkColumn("Abrir", help="Abrir no Jira"),
                    "Resumo": st.column_config.TextColumn("Resumo", width="large", max_chars=120),
                },
            )
        except Exception as e:
            st.error(f"Erro ao renderizar dataframe: {e}")
            st.dataframe(
                df[["Chamado", "Resumo", "Status", "Responsável", "Atualizado", "URL"]],
                use_container_width=True, height=380, hide_index=True
            )
    else:
        st.warning("⚠️ Nenhum chamado aberto encontrado ou erro na consulta.")
        st.caption("Verifique se há tickets no projeto TD com status 'To Do' ou 'In Progress'")

    st.markdown('</div>', unsafe_allow_html=True)


def render_kpis_card(kpis_data: dict):
    """Renderiza card dos KPIs com layout moderno em cards"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("📊 KPIs Evino")
    
    # Indicador de atualização
    st.caption(f"📅 Dados atualizados em: {datetime.now(TZ).strftime('%d/%m/%Y %H:%M')}")

    # Verificar se os dados estão disponíveis
    if not kpis_data:
        st.error("❌ Nenhum dado de KPI disponível")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    # Extrair dados com tratamento de erro
    try:
        today = kpis_data.get("today", {})
        month = kpis_data.get("month", {})
        top = kpis_data.get("top", {})
        fc_day = kpis_data.get("fc_day", {})
        fc_mon = kpis_data.get("fc_mon", {})
        expected_revenue = kpis_data.get("expected_revenue", 0)
        expected_month_revenue = kpis_data.get("expected_month_revenue", 0)
        hora_pedido = kpis_data.get("hora_pedido", "N/A")
        diff_min = kpis_data.get("diff_min", None)
    except Exception as e:
        st.error(f"❌ Erro ao processar dados de KPI: {str(e)}")
        st.markdown('</div>', unsafe_allow_html=True)
        return

    # ======================== CARDS PRINCIPAIS ========================
    st.markdown("### 💰 Receita e Performance")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        today_revenue = today.get('today_revenue', 0) or 0
        last_hour_revenue = today.get('last_hour_revenue', 0) or 0
        st.metric(
            "Receita Hoje",
            f"R$ {today_revenue:,.0f}".replace(",", "."),
            f"R$ {last_hour_revenue:,.0f}".replace(",", "."),
            help="Receita total do dia atual (última hora)"
        )
    
    with col2:
        cm2_value = today.get('cm2', 0) or 0
        cm2_pct = cm2_value * 100
        st.metric(
            "CM2 Hoje",
            f"{cm2_pct:.1f}%",
            help="Contribuição marginal 2 do dia atual"
        )
    
    with col3:
        today_forecast = fc_day.get('today_forecast', 0) or 0
        st.metric(
            "Meta do Dia",
            f"R$ {today_forecast:,.0f}".replace(",", "."),
            help="Meta de receita para o dia atual"
        )
    
    with col4:
        progress_pct = (expected_revenue / today_forecast * 100) if today_forecast > 0 else 0
        st.metric(
            "Progresso",
            f"{progress_pct:.1f}%",
            help="Percentual da meta atingido até agora"
        )
    
    st.divider()
    
    # ======================== CARDS SECUNDÁRIOS ========================
    st.markdown("### 📈 Dados Mensais e Top Sellers")
    
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        month_revenue_flash = month.get('month_revenue_flash_sale', 0) or 0
        st.metric(
            "Receita Flash Mês",
            f"R$ {month_revenue_flash:,.0f}".replace(",", "."),
            help="Receita de flash sales do mês"
        )
    
    with col6:
        month_cm2_value = month.get('month_cm2', 0) or 0
        month_cm2_pct = month_cm2_value * 100
        st.metric(
            "CM2 Mês",
            f"{month_cm2_pct:.1f}%",
            help="Contribuição marginal 2 do mês"
        )
    
    with col7:
        month_forecast = fc_mon.get('month_forecast', 0) or 0
        st.metric(
            "Meta do Mês",
            f"R$ {month_forecast:,.0f}".replace(",", "."),
            help="Meta de receita para o mês"
        )
    
    with col8:
        month_progress_pct = (expected_month_revenue / month_forecast * 100) if month_forecast > 0 else 0
        st.metric(
            "Progresso Mês",
            f"{month_progress_pct:.1f}%",
            help="Percentual da meta mensal atingido"
        )
    
    st.divider()
    
    # ======================== INFORMAÇÕES ADICIONAIS ========================
    st.markdown("### 📊 Informações Detalhadas")
    
    col_info1, col_info2 = st.columns(2)
    
    with col_info1:
        st.markdown("#### 🏆 Top Seller (Última Hora)")
        top_seller = top.get('top_seller', 'N/A')
        bottles = top.get('bottles', 0) or 0
        st.info(f"**{top_seller}** - {bottles} garrafas vendidas")
        
        st.markdown("#### ⏰ Último Pedido")
        st.info(f"**{hora_pedido}**")
        if diff_min is not None:
            if diff_min > 30:
                st.warning(f"⚠️ Há {diff_min:.0f} minutos sem pedidos")
            else:
                st.success(f"✅ Último pedido há {diff_min:.0f} minutos")
        else:
            st.info("ℹ️ Tempo desde último pedido não disponível")
    
    with col_info2:
        st.markdown("#### 📋 Resumo Executivo")
        
        # Status geral
        if progress_pct >= 100:
            st.success("🎯 **Meta do dia atingida!**")
        elif progress_pct >= 80:
            st.info("📈 **Bom progresso** - próximo da meta")
        elif progress_pct >= 50:
            st.warning("⚠️ **Progresso moderado** - atenção necessária")
        else:
            st.error("🚨 **Progresso baixo** - ação imediata necessária")
        
        # Comparação mensal
        if month_progress_pct >= 100:
            st.success("🎯 **Meta mensal atingida!**")
        elif month_progress_pct >= 80:
            st.info("📈 **Bom progresso mensal**")
        else:
            st.warning("⚠️ **Meta mensal em risco**")
    
    st.divider()
    
    # ======================== STATUS DE ATUALIZAÇÃO ========================
    st.markdown("### 🔄 Status de Atualização")
    
    if diff_min is not None and diff_min > 30:
        st.warning(f"⚠️ **Painel pode estar defasado** — último pedido foi há {diff_min:.0f} minutos")
    elif diff_min is not None:
        st.success(f"📦 **Dados atualizados** — último pedido há {diff_min:.0f} minutos")
    else:
        st.info("ℹ️ **Status de atualização não disponível**")
    
    st.caption(f"📅 Snapshot gerado em: {kpis_data.get('now', datetime.now(TZ)).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Seção de debug (expansível)
    with st.expander("🔧 Debug - Dados Brutos", expanded=False):
        st.json(kpis_data)
    
    st.markdown('</div>', unsafe_allow_html=True)


def render_kestra_card(flow_ids: list = None):
    """Renderiza card de status dos flows do Kestra"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("🔄 Status dos Flows Kestra")
    
    # Verificar se há configuração do Kestra
    try:
        kestra_configured = bool(st.secrets.get("kestra", {}).get("base_url") and 
                                st.secrets.get("kestra", {}).get("api_key"))
    except Exception:
        kestra_configured = False
    
    if not kestra_configured:
        st.warning("⚠️ Kestra não configurado. Adicione as credenciais em secrets.toml")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    # Verificar se a URL está funcionando
    base_url = st.secrets.get("kestra", {}).get("base_url", "")
    if "kestra.vissimo.tech" in base_url:
        st.error("🚨 **URL antiga detectada!**")
        st.markdown("""
        **Problema:**
        - ❌ URL antiga: `kestra.vissimo.tech` (não funciona)
        - ✅ URL nova: `api.evino.com.br/kestra` (funcionando)
        
        **Solução:**
        Atualize o `base_url` no `secrets.toml` para:
        ```
        base_url = "https://api.evino.com.br/kestra"
        ```
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    # Importar funções do Kestra
    from monitor_dw.services.kestra_client import (
        get_kestra_flows, get_flow_status_from_docs, trigger_kestra_flow
    )
    
    # Testar conexão com endpoint da documentação oficial
    st.info("🔧 **Testando integração com Kestra usando documentação oficial**")
    
    # Seção para testar flow específico
    with st.expander("🧪 Teste de Flow Específico", expanded=True):
        col1, col2 = st.columns([2, 1])
        
        with col1:
            flow_id = st.text_input(
                "Flow ID para testar:",
                value="rpa_vf_clientes_ev",
                help="Digite o ID do flow que você quer monitorar"
            )
        
        with col2:
            namespace = st.text_input(
                "Namespace:",
                value="rpa.varejofacil",
                help="Namespace do flow (baseado na interface do Kestra)"
            )
        
        if st.button("🔍 Testar Flow", type="primary"):
            with st.spinner("Testando conexão com Kestra..."):
                flow_status = get_flow_status_from_docs(flow_id, namespace)
                
                if flow_status.get("status") == "SUCCESS":
                    st.success("✅ **Conexão bem-sucedida!**")
                    
                    col3, col4 = st.columns(2)
                    with col3:
                        st.metric("Flow ID", flow_status.get("flow_id"))
                        st.metric("Namespace", flow_status.get("namespace"))
                    
                    with col4:
                        st.metric("Execuções", flow_status.get("execution_count", 0))
                        if flow_status.get("state"):
                            st.metric("Estado", flow_status.get("state"))
                    
                    if flow_status.get("latest_execution"):
                        st.markdown("**Última execução:**")
                        latest = flow_status["latest_execution"]
                        st.json(latest)
                        
                elif flow_status.get("status") == "NO_EXECUTIONS":
                    st.info("ℹ️ **Flow encontrado, mas sem execuções**")
                    st.write(f"**Flow ID**: {flow_status.get('flow_id')}")
                    st.write(f"**Namespace**: {flow_status.get('namespace')}")
                    
                else:
                    st.error(f"❌ **Erro**: {flow_status.get('message', 'Erro desconhecido')}")
    
    # Seção de teste manual
    with st.expander("🧪 Teste Manual com cURL", expanded=False):
        st.markdown("""
        **Para testar manualmente usando a documentação oficial:**
        
        ```bash
        # Testar endpoint de flows
        curl -H "X-EVINO-KESTRA-API-KEY: ELFkM8LTsLpTmbvzVyVqCslVDfBdNAcP" \\
             -H "Content-Type: application/json" \\
             -H "Accept: application/json" \\
             "https://api.evino.com.br/kestra/api/v1/main/flows"
        
        # Testar endpoint de execuções de um flow específico
        curl -H "X-EVINO-KESTRA-API-KEY: ELFkM8LTsLpTmbvzVyVqCslVDfBdNAcP" \\
             -H "Content-Type: application/json" \\
             -H "Accept: application/json" \\
             "https://api.evino.com.br/kestra/api/v1/main/executions/flows/main/SEU_FLOW_ID"
        ```
        
        **Endpoints baseados na documentação oficial:**
        - `GET /api/v1/{tenant}/flows` - Listar flows
        - `GET /api/v1/{tenant}/executions/flows/{namespace}/{flowId}` - Execuções de um flow
        - `GET /api/v1/{tenant}/executions/{executionId}` - Detalhes de uma execução
        """)
    
    st.markdown('</div>', unsafe_allow_html=True)
    return
    
    # Seção de configuração
    with st.expander("⚙️ Configuração", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Atualizar Lista de Flows"):
                st.cache_data.clear()
                st.rerun()
        
        with col2:
            if st.button("📋 Listar Todos os Flows"):
                flows = get_kestra_flows()
                if flows:
                    st.write(f"**{len(flows)} flows encontrados:**")
                    for flow in flows[:10]:  # Mostrar apenas os primeiros 10
                        st.write(f"- {flow.get('id', 'N/A')}")
                    if len(flows) > 10:
                        st.write(f"... e mais {len(flows) - 10} flows")
                else:
                    st.write("Nenhum flow encontrado")
    
    # Seção de monitoramento
    st.markdown("### 📊 Monitoramento de Flows")
    
    # Se não foram especificados flows, tentar obter automaticamente
    if not flow_ids:
        flows = get_kestra_flows()
        if flows:
            # Pegar os primeiros 5 flows como exemplo
            flow_ids = [flow.get("id") for flow in flows[:5] if flow.get("id")]
        else:
            st.info("ℹ️ Nenhum flow encontrado. Configure flows específicos ou verifique a conexão.")
            st.markdown('</div>', unsafe_allow_html=True)
            return
    
    # Obter status dos flows
    flows_status = get_multiple_flows_status(flow_ids)
    
    # Exibir status de cada flow
    for flow_id, status_info in flows_status.items():
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
            
            with col1:
                st.write(f"**{flow_id}**")
            
            with col2:
                status = status_info.get("status", "UNKNOWN")
                if status == "SUCCESS":
                    st.success("✅ Sucesso")
                elif status == "FAILED":
                    st.error("❌ Falhou")
                elif status == "RUNNING":
                    st.info("🔄 Executando")
                elif status == "KILLED":
                    st.warning("⏹️ Cancelado")
                else:
                    st.info(f"ℹ️ {status}")
            
            with col3:
                last_run = status_info.get("last_run")
                if last_run:
                    try:
                        # Converter timestamp para datetime
                        dt = datetime.fromisoformat(last_run.replace('Z', '+00:00'))
                        st.write(f"🕒 {dt.strftime('%d/%m %H:%M')}")
                    except Exception:
                        st.write("🕒 N/A")
                else:
                    st.write("🕒 N/A")
            
            with col4:
                if st.button("▶️", key=f"trigger_{flow_id}", help="Disparar flow"):
                    result = trigger_kestra_flow(flow_id)
                    if result["success"]:
                        st.success("✅ Disparado!")
                    else:
                        st.error(f"❌ {result['message']}")
                    st.rerun()
            
            # Mostrar detalhes adicionais
            with st.expander(f"📋 Detalhes - {flow_id}", expanded=False):
                st.json(status_info)
    
    # Resumo geral
    st.divider()
    st.markdown("### 📈 Resumo")
    
    total_flows = len(flows_status)
    success_count = sum(1 for status in flows_status.values() if status.get("status") == "SUCCESS")
    failed_count = sum(1 for status in flows_status.values() if status.get("status") == "FAILED")
    running_count = sum(1 for status in flows_status.values() if status.get("status") == "RUNNING")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Flows", total_flows)
    
    with col2:
        st.metric("Sucessos", success_count)
    
    with col3:
        st.metric("Falhas", failed_count)
    
    with col4:
        st.metric("Executando", running_count)
    
    st.markdown('</div>', unsafe_allow_html=True)


def render_slack_diagnostic_card():
    """Renderiza card de diagnóstico do Slack"""
    st.markdown("### 🔔 Diagnóstico do Slack")
    
    try:
        WEBHOOK_URL = st.secrets["slack"]["webhook_url"].strip()
    except Exception:
        WEBHOOK_URL = ""
    
    # Mostrar informações do webhook (parcialmente mascarado)
    if WEBHOOK_URL:
        masked_url = WEBHOOK_URL[:30] + "..." + WEBHOOK_URL[-10:] if len(WEBHOOK_URL) > 40 else WEBHOOK_URL
        st.info(f"🔗 Webhook configurado: `{masked_url}`")
        
        # Validação básica da URL
        if not WEBHOOK_URL.startswith("https://hooks.slack.com/services/"):
            st.warning("⚠️ URL do webhook não parece estar no formato correto do Slack")
        else:
            st.success("✅ Formato da URL do webhook parece correto")
            
        # Verificar se há resultado de teste recente que indica webhook revogado
        _res = st.session_state.get("slack_test_result")
        if _res and (time.time() - float(_res.get("ts", 0)) < 300) and "REVOGADO" in _res.get('info', ''):
            st.error("🚨 **ATENÇÃO: Webhook foi REVOGADO!** Você precisa criar um novo webhook no Slack.")
    else:
        st.error("❌ Webhook não configurado")

    test_msg = st.text_input("Mensagem de teste", "Teste do Monitor DW ✔️")
    webhook_override = st.text_input("Webhook (opcional, sobrescrever)", value="", type="password", help="Cole aqui um webhook para testar sem alterar a configuração global")
    send_clicked = st.button("Enviar teste para Slack")
    
    if send_clicked:
        from ..services.alerts import test_slack_webhook
        ok, info = test_slack_webhook(test_msg, webhook_override)
        
        # Toast
        try:
            st.toast(f"Slack: {'ok' if ok else 'falhou'} — {info}")
        except Exception:
            pass

    # Show last test status for up to 60s
    _res = st.session_state.get("slack_test_result")
    if _res and (time.time() - float(_res.get("ts", 0)) < 60):
        if _res.get("ok"):
            st.success(f"Slack teste: ok — {_res.get('info')}")
        else:
            st.error(f"Slack teste: falhou — {_res.get('info')}")
            
            # Mostrar instruções específicas se o webhook foi revogado
            if "REVOGADO" in _res.get('info', ''):
                st.markdown("### 🔧 Como criar um novo webhook no Slack:")
                st.markdown("""
                1. **Acesse o Slack** e vá para o canal onde quer receber os alertas
                2. **Clique no nome do canal** → "Configurações" → "Integrações"
                3. **Procure por "Incoming Webhooks"** e clique em "Adicionar"
                4. **Clique em "Adicionar integração Incoming Webhooks"**
                5. **Copie a URL do webhook** (começa com `https://hooks.slack.com/services/...`)
                6. **Cole a nova URL** no campo "Webhook (opcional, sobrescrever)" acima
                7. **Teste novamente** clicando em "Enviar teste para Slack"
                """)
            else:
                st.caption("Dicas: 1) Confirme se o webhook é do app Incoming Webhooks do workspace certo. 2) Verifique se não foi revogado. 3) Gere um novo e cole acima.")
