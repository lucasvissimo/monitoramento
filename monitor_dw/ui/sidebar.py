# -*- coding: utf-8 -*-
"""
Componentes de sidebar e controles
"""

import streamlit as st
import time
from ..config import TZ, REDSHIFT_THRESHOLD_MIN, REFRESH_ALERT_MIN, AUTO_REFRESH_SEC
from ..db import get_redshift_conn, get_postgres_conn
from datetime import datetime


def render_auth_sidebar():
    """Renderiza controles de autenticação na sidebar"""
    st.sidebar.header("⚙️ Configurações")
    st.sidebar.caption(f"👤 Logado como: **{st.session_state['auth_user']}**")
    
    # Appearance toggles
    with st.sidebar.expander("Aparência", expanded=False):
        compact_mode = st.checkbox("Modo compacto", value=False, key="ui_compact")
        high_contrast = st.checkbox("Alto contraste", value=False, key="ui_contrast")
    
    # Manual refresh
    if st.sidebar.button("Atualizar agora", help="Recarrega a página imediatamente"):
        st.rerun()

    # Apply appearance styles
    if compact_mode:
        st.markdown(
            """
            <style>
            html, body, [class^="css"] { font-size: 14px; }
            .metric { padding: 10px; border-radius: 10px; }
            .metric .value { font-size: 1.35rem; }
            .card { padding: 14px; border-radius: 12px; }
            </style>
            """,
            unsafe_allow_html=True,
        )
    if high_contrast:
        st.markdown(
            """
            <style>
            .card { border-color: #ffffff55; }
            .metric { border-color: #ffffff66; }
            .stDataFrame { box-shadow: 0 0 0 1px #ffffff55 inset; }
            </style>
            """,
            unsafe_allow_html=True,
        )

    if st.sidebar.button("Sair"):
        st.session_state["auth_user"] = None
        st.rerun()


def render_auto_refresh_controls() -> tuple[int, int, bool, int]:
    """Renderiza controles de auto-refresh e retorna configurações"""
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

    if auto_refresh:
        try:
            now_ts = time.time()
            suspend_until = float(st.session_state.get("suspend_auto_refresh_until", 0) or 0)
            if now_ts < suspend_until:
                st.sidebar.markdown("<span class=\"badge warn\">⏸️ Auto (pausado)</span>", unsafe_allow_html=True)
            else:
                st.autorefresh(interval=int(auto_refresh_sec) * 1000, key="auto_rf")
                st.sidebar.markdown("<span class=\"badge ok\">🔄 Auto</span>", unsafe_allow_html=True)
        except Exception:
            st.sidebar.info("Sua versão do Streamlit não tem auto-refresh nativo. Use o botão 'Atualizar'.")

    return redshift_threshold, refresh_alert_min, auto_refresh, auto_refresh_sec


def render_system_info():
    """Renderiza informações do sistema na sidebar"""
    with st.sidebar.expander("ℹ️ Informações do Sistema", expanded=False):
        st.caption(f"🕐 Hora atual: {datetime.now(TZ).strftime('%H:%M:%S')}")
        st.caption(f"📅 Data: {datetime.now(TZ).strftime('%d/%m/%Y')}")
        st.caption(f"🌍 Timezone: {TZ}")
        st.caption(f"👤 Usuário: {st.session_state.get('auth_user', 'Não logado')}")
        
        # Status das conexões
        st.caption("🔗 Status das conexões:")
        try:
            _ = get_redshift_conn()
            st.caption("✅ Redshift: Conectado")
        except:
            st.caption("❌ Redshift: Desconectado")
        
        try:
            _ = get_postgres_conn()
            st.caption("✅ Postgres: Conectado")
        except:
            st.caption("❌ Postgres: Desconectado")


def render_auth_ui():
    """Renderiza interface de autenticação"""
    import os
    import json
    import hashlib
    from ..config import USERS_DB_PATH, TZ

    @st.cache_resource(show_spinner=False)
    def _load_users() -> dict:
        if not os.path.exists(USERS_DB_PATH):
            with open(USERS_DB_PATH, "w", encoding="utf-8") as f:
                json.dump({"users": []}, f)
        with open(USERS_DB_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_users(db: dict) -> None:
        with open(USERS_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False, indent=2)

    DEF_ITER = 100000

    def _hash_pw(password: str, salt: str) -> str:
        return hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), DEF_ITER).hex()

    # UI de login/cadastro
    if "auth_user" not in st.session_state:
        st.session_state["auth_user"] = None

    if st.session_state["auth_user"] is None:
        tab_login, tab_signup = st.tabs(["🔐 Entrar", "📝 Cadastrar-se"])

        with tab_login:
            st.markdown('<div class="auth-card">', unsafe_allow_html=True)
            st.markdown("<h3>Bem-vindo de volta</h3>", unsafe_allow_html=True)
            lu = st.text_input("Usuário", key="login_usuario")
            lp = st.text_input("Senha", type="password", key="login_senha")
            if st.button("Entrar", type="primary", key="btn_entrar"):
                db = _load_users()
                user = next((u for u in db.get("users", []) if u.get("user") == lu), None)
                if user:
                    salt = user.get("salt")
                    if _hash_pw(lp, salt) == user.get("hash"):
                        st.session_state["auth_user"] = lu
                        from ..db import log_user_login
                        log_user_login(lu)  # Log successful login
                        st.rerun()
                    else:
                        st.error("Senha inválida.")
                else:
                    st.error("Usuário não encontrado.")
            st.markdown('</div>', unsafe_allow_html=True)

        with tab_signup:
            st.markdown('<div class="auth-card">', unsafe_allow_html=True)
            st.markdown("<h3>Criar conta</h3>", unsafe_allow_html=True)
            su  = st.text_input("Usuário novo", key="signup_usuario")
            sp  = st.text_input("Senha", type="password", key="signup_senha")
            sp2 = st.text_input("Confirmar senha", type="password", key="signup_confirma_senha")
            if st.button("Cadastrar", key="btn_cadastrar"):
                if not su or not sp:
                    st.warning("Informe usuário e senha.")
                elif sp != sp2:
                    st.error("Senhas não conferem.")
                else:
                    db = _load_users()
                    if any(u.get("user") == su for u in db.get("users", [])):
                        st.error("Usuário já existe.")
                    else:
                        salt = os.urandom(16).hex()
                        db.setdefault("users", []).append({
                            "user": su,
                            "salt": salt,
                            "hash": _hash_pw(sp, salt),
                            "created_at": datetime.now(TZ).isoformat(),
                        })
                        _save_users(db)
                        st.success("Conta criada! Faça login na aba Entrar.")
            st.markdown('</div>', unsafe_allow_html=True)

        st.stop()
