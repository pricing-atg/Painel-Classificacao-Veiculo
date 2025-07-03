from PIL import Image
import streamlit as st
from authentication.login import login_user
from paineis.painel_class_veic import *

# Configura página
st.set_page_config(
    page_title="Painel de Classificação de Veículos",
    layout="wide",
    page_icon=Image.open("img/favicon.ico"),
)

# Carrega autenticação
authenticator = login_user()

auth_status = st.session_state.get("authentication_status", None)

if auth_status:

    # Boas-vindas e botão logout lado a lado
    col1, col2 = st.columns([9, 1])
    with col1:
        st.write(f'Seja bem-vindo(a), membro da equipe de {st.session_state["name"]}!')
    with col2:
        authenticator.logout(location='main')

    # Exibe painel conforme tipo de usuário
    usuario_tipo = st.session_state.get("email")
    if usuario_tipo == "pricing":
        show_painel_classificaco_veiculo()
    else:
        st.error("Tipo de usuário desconhecido.")

elif auth_status is False:
    st.error("Usuário ou senha incorretos.")
    # Exibe imagem e formulário login para nova tentativa
    st.image("img/logo-autoglass-maxpar-1.png", use_container_width=True)
    st.markdown(
        "<h3 style='text-align: center; margin-top: 10px;'>Painel de Proposta Padrão</h3>",
        unsafe_allow_html=True,
    )
    
    st.write("")
    
    authenticator.login(
        location='main',
        fields={
            "Form name": "Acesso ao Sistema",
            "Username": "Usuário",
            "Password": "Senha"
        }
    )

else:
    # Primeiro acesso - imagem + login
    col1, col2, col3 = st.columns([1,1,1])
    with col2:
        st.image("img/logo-autoglass-maxpar-1.png",use_container_width=True)
    st.markdown(
        "<h3 style='text-align: center; margin-top: 10px;'>Painel de Proposta Padrão</h3>",
        unsafe_allow_html=True,
    )
    
    st.write("")
    
    authenticator.login(
        location='main',
        fields={
            "Form name": "Acesso ao Sistema",
            "Username": "Usuário",
            "Password": "Senha"
        }
    )