import streamlit as st
import pandas as pd
import snowflake.connector
import duckdb
import numpy as np

from authentication.login import login_user  # Manteve-se pois é do projeto

def show_painel_classificaco_veiculo():
    st.title("Painel de Classificação de Veículos")
    
    # Importar a base de calssificação dos veículos
    
    # Caminho da base
    caminho_base_classif = 'data/df_classif.parquet'
    
    # Conectando ao DuckDB em memória
    con = duckdb.connect(database=':memory:')
    
    query_importar_base_classif = f"""
        SELECT DISTINCT *
        FROM parquet_scan('{caminho_base_classif}')
    """

    # Executando a consulta e fechando a conexão
    base_classif = con.execute(query_importar_base_classif).fetchdf()
    con.close()
    
    variavel_ano_modelo = base_classif["Ano"].unique()
    variavel_valor = base_classif["Valor"].unique()
    variavel_tipo_veiculo = base_classif["Carga/Moto/Auto"].unique()
    
    variavel_cabrio_conv = base_classif["Cabrio/Convencional"].unique()
    variavel_tipo_veic_cat = base_classif["Elétrico/Híbrido/Convencional"].unique()
    # Caso selecione Blindados vai a variável de "Blindados" e selecionar Sim
    variavel_restricao = np.unique(
            np.concatenate([variavel_cabrio_conv, variavel_tipo_veic_cat, ["Blindados"]])
        )
    
    st.write("Logo abaixo pode ser aplicado os filtros para restringir a base de classificação.")
    
    
    with st.container():
        coluna01, coluna02 = st.columns(2)        

        with coluna01:
            tipos_veiculos_selecionados = st.multiselect(
                "Tipo de Veículo",
                options=sorted(variavel_tipo_veiculo),
                default=sorted(variavel_tipo_veiculo),
                placeholder="Selecione os tipos de veículo"
            )
            
        with coluna02:
            tipos_restricao = st.multiselect(
                "Categorias de Restrição",
                options=sorted(variavel_restricao),
                default=None,
                placeholder="Selecione as Restrições dos Veículos"
            )
    
    
    with st.container():
        coluna01, coluna02 = st.columns(2)

        with coluna01:
            
            variavel_ano_modelo = sorted(variavel_ano_modelo)

            ano_modelo_min, ano_modelo_max = st.select_slider(
                "Ano do Modelo",
                options=variavel_ano_modelo,
                value=(variavel_ano_modelo[0], variavel_ano_modelo[-1])
            )

        with coluna02:
            valores_disponiveis = sorted(variavel_valor)
            valor_max = int(max(valores_disponiveis)) if len(valores_disponiveis) > 0 else 50000
            valores_intervalados = list(range(0, valor_max + 50000, 50000))

            valor_min, valor_max = st.select_slider(
                "Valor do Veículo",
                options=valores_intervalados,
                value=(valores_intervalados[0], valores_intervalados[-1]),
                format_func=lambda x: f"R$ {x:,.0f}".replace(",", ".")
            )
            
    # Aplicar os filtros com base nas seleções
    base_filtrada = base_classif.copy()

    # 1. Filtro por Ano
    base_filtrada = base_filtrada[
        (base_filtrada["Ano"] >= ano_modelo_min) &
        (base_filtrada["Ano"] <= ano_modelo_max)
    ]

    # 2. Filtro por Valor
    base_filtrada = base_filtrada[
        (base_filtrada["Valor"] >= valor_min) &
        (base_filtrada["Valor"] <= valor_max)
    ]

    # 3. Filtro por Tipo de Veículo
    base_filtrada = base_filtrada[
        base_filtrada["Carga/Moto/Auto"].isin(tipos_veiculos_selecionados)
    ]

    # 4. Filtro por restrições (exclui os registros conforme a seleção)
    if tipos_restricao:

        if "Convencional" in tipos_restricao:
            base_filtrada = base_filtrada[
                ~(
                    base_filtrada["Cabrio/Convencional"].str.upper().eq("CONVENCIONAL") |
                    base_filtrada["Elétrico/Híbrido/Convencional"].str.upper().eq("CONVENCIONAL")
                )
            ]

        if "Cabrio" in tipos_restricao:
            base_filtrada = base_filtrada[
                ~base_filtrada["Cabrio/Convencional"].str.upper().eq("CABRIO")
            ]

        if "Elétrico" in tipos_restricao:
            base_filtrada = base_filtrada[
                ~base_filtrada["Elétrico/Híbrido/Convencional"].str.upper().eq("ELÉTRICO")
            ]

        if "Híbrido" in tipos_restricao:
            base_filtrada = base_filtrada[
                ~base_filtrada["Elétrico/Híbrido/Convencional"].str.upper().eq("HÍBRIDO")
            ]

        if "Blindados" in tipos_restricao:
            base_filtrada = base_filtrada[
                ~base_filtrada["Blindados"].eq("Sim")
            ]
    
    st.markdown("### Definir Classificação por Faixa de Valor")

    # Valor mínimo e máximo da base arredondados para múltiplos de 5.000
    valor_min_base = int(base_filtrada["Valor"].min() // 5000 * 5000)
    valor_max_base = int(np.ceil(base_filtrada["Valor"].max() / 5000) * 5000)

    # Input para número de categorias
    num_categorias = st.number_input(
        "Número de Categorias",
        min_value=1,
        max_value=10,
        value=1,
        step=1
    )

    if num_categorias == 1:
        df_faixas = pd.DataFrame({
            "Categoria": ["Cat. Única"],
            "valor_inicial": [valor_min_base],
            "valor_final": [valor_max_base]
        })
    else:
        categorias = [f"Cat. {chr(65 + i)}" for i in range(num_categorias)]

        # Gera limites igualmente espaçados
        limites = np.linspace(valor_min_base, valor_max_base, num_categorias + 1)

        # Arredonda os limites para múltiplos de 5.000
        limites = [int(round(x / 5000.0) * 5000) for x in limites]

        # Garante que os limites são crescentes e não repetidos
        for i in range(1, len(limites)):
            if limites[i] <= limites[i - 1]:
                limites[i] = limites[i - 1] + 5000

        valor_iniciais = limites[:-1]
        valor_finais = limites[1:]

        df_faixas = pd.DataFrame({
            "Categoria": categorias,
            "valor_inicial": valor_iniciais,
            "valor_final": valor_finais
        })

    # Editor interativo com step de R$ 5.000
    df_faixas_editado = st.data_editor(
        df_faixas,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "valor_inicial": st.column_config.NumberColumn("Valor Inicial", format="R$ %d", step=5000),
            "valor_final": st.column_config.NumberColumn("Valor Final", format="R$ %d", step=5000),
        }
    )
    
    # Aplicar a classificação por faixa de valor
    def classificar_por_faixa(valor, faixas):
        for _, row in faixas.iterrows():
            if row["valor_inicial"] <= valor <= row["valor_final"]:
                return row["Categoria"]
        return "Fora da faixa"

    # Criar nova coluna 'categoria_valor' na base_filtrada
    base_filtrada["Categoria Valor"] = base_filtrada["Valor"].apply(
        lambda x: classificar_por_faixa(x, df_faixas_editado)
    )
    
    # Se só houver uma categoria de valor, atribui diretamente
    if base_filtrada["Categoria Valor"].nunique() == 1:
        base_filtrada["Categoria Ajustada"] = base_filtrada["Categoria Valor"]
        st.markdown("### Todas as observações pertencem à mesma categoria. Ajuste não é necessário.")
        st.dataframe(base_filtrada)
    else:
    
        # Etapa 0: Garantir que as colunas estejam no formato correto
        df = base_filtrada.copy()
        df = df[["MF Reformulada por Fipe e Ano", "Valor", "Categoria Valor"]].dropna()

        # Etapa 1: Contagem de categorias por MF Reformulada
        contagem = (
            df.groupby(["MF Reformulada por Fipe e Ano", "Categoria Valor"])
            .size()
            .reset_index(name="frequencia")
        )

        # Identificar a(s) categoria(s) mais frequente(s) por MF
        cat_max_freq = contagem.groupby("MF Reformulada por Fipe e Ano").apply(
            lambda x: x[x["frequencia"] == x["frequencia"].max()]
        ).reset_index(drop=True)

        # Separar os casos resolvidos diretamente (frequência não empatada)
        casos_unicos = cat_max_freq.groupby("MF Reformulada por Fipe e Ano").filter(lambda x: len(x) == 1)
        casos_unicos["Categoria Ajustada"] = casos_unicos["Categoria Valor"]

        # Etapa 2: Tratar empates pela média do valor
        casos_empate = cat_max_freq.groupby("MF Reformulada por Fipe e Ano").filter(lambda x: len(x) > 1)

        # Média geral por categoria
        media_geral_categoria = df.groupby("Categoria Valor")["Valor"].mean().to_dict()

        # Média local por MF e categoria
        media_local = (
            df[df["MF Reformulada por Fipe e Ano"].isin(casos_empate["MF Reformulada por Fipe e Ano"])]
            .groupby(["MF Reformulada por Fipe e Ano", "Categoria Valor"])["Valor"]
            .mean()
            .reset_index(name="media_local")
        )

        # Anexar a média geral
        media_local["media_geral"] = media_local["Categoria Valor"].map(media_geral_categoria)

        # Calcular a diferença absoluta
        media_local["dif_absoluta"] = (media_local["media_local"] - media_local["media_geral"]).abs()

        # Selecionar a menor diferença para definir a Categoria Ajustada
        cat_ajustada_empate = (
            media_local.sort_values("dif_absoluta")
            .groupby("MF Reformulada por Fipe e Ano")
            .first()
            .reset_index()[["MF Reformulada por Fipe e Ano", "Categoria Valor"]]
        )
        cat_ajustada_empate["Categoria Ajustada"] = cat_ajustada_empate["Categoria Valor"]

        # Concatenar os resultados
        resultado_final = pd.concat([
            casos_unicos[["MF Reformulada por Fipe e Ano", "Categoria Ajustada"]],
            cat_ajustada_empate[["MF Reformulada por Fipe e Ano", "Categoria Ajustada"]]
        ], ignore_index=True)

        # Juntar na base original
        base_final = base_filtrada.merge(resultado_final, on="MF Reformulada por Fipe e Ano", how="left")

        # Exibir no Streamlit
        st.markdown("### Base com Categoria Ajustada")
        st.dataframe(base_final, hide_index = True)
    