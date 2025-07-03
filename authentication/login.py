import os
import yaml
from yaml.loader import SafeLoader
import streamlit as st
import streamlit_authenticator as stauth

def login_user():
    """Carrega o config.yaml e retorna o objeto authenticator"""
    try:
        config_path = os.path.join(
            os.path.dirname(__file__), "..", ".streamlit", "config.yaml"
        )
        # st.info(f"Lendo config.yaml de: {config_path}")
        with open(config_path) as file:
            config = yaml.load(file, Loader=SafeLoader)

        authenticator = stauth.Authenticate(
            config["credentials"],
            config["cookie"]["name"],
            config["cookie"]["key"],
            config["cookie"]["expiry_days"],
        )

        return authenticator

    except Exception as e:
        st.error(f"Erro ao carregar autenticação: {e}")
        import traceback
        st.text(traceback.format_exc())
        return None
