import marimo

__generated_with = "0.21.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd

    return mo, pd


@app.cell
def _(pd):
    def carregar_dados(arquivo, tipo):
        if tipo == 'csv':
            df = pd.read_csv(arquivo)
        elif tipo == 'json':
            df = pd.read_json(arquivo)
        return df

    return (carregar_dados,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Q1: EDA**
    """)
    return


@app.cell
def _(carregar_dados):
    vendas = carregar_dados('data/vendas_2023_2024.csv', 'csv')
    vendas.head()
    return (vendas,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q1.1: SQL**
    Script SQL calculando:

    - Quantidade total de linhas
    - Quantidade total de colunas
    - Intervalo de datas analisado (data mínima e máxima)
    - Valor mínimo
    - Valor máximo
    - Valor médio
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    A tabela possui **9895** linhas e **6** colunas, com registros de vendas no período de **01/01/2023 a 31/12/2024**.

    A coluna **`total`** tem o valor mínimo de **294,5** e máximo de **2222973**, com média de **263797.83**.
    """)
    return


@app.cell
def profiling(mo, vendas):
    _df = mo.sql(
        f"""
        -- Profiling com função exclusiva do DuckDB
        SUMMARIZE vendas;
        """
    )
    return


@app.cell
def _(mo, vendas):
    _df = mo.sql(
        f"""
        -- Quantidade de linhas
        SELECT COUNT(*) FROM vendas;
        """
    )
    return


@app.cell
def _(mo, vendas):
    _df = mo.sql(
        f"""
        -- Quantidade e tipo de dados de cada coluna
        DESCRIBE vendas;
        """
    )
    return


@app.cell
def _(mo, vendas):
    _df = mo.sql(
        f"""
        -- Mínimos, Máximos e Média (total)
        SELECT 
          MIN(sale_date) AS primeira_data, 
          MAX(sale_date) AS ultima_data, 
          MIN(total) AS total_minimo, 
          MAX(total) AS total_macimo, 
          ROUND(AVG(total), 2) AS total_media
        FROM vendas;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q1.2: Validação**
    Qual é o valor máximo registrado na coluna "total"?
    """)
    return


@app.cell
def _(mo, vendas):
    _df = mo.sql(
        f"""
        -- Valor máximo da coluna total
        SELECT MAX(total) FROM vendas;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    O valor máximo registrado na coluna **`total`** **2222973**.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q1.3: Interpretação**
    Com base na análise exploratória realizada, escreva um breve diagnóstico sobre a confiabilidade do dataset vendas_2023_2024.csv para análises futuras. Comente sobre:

    - Possíveis outliers em "total",
    - Qualidade dos dados (valores nulos ou inconsistentes),
    - e se você considera que o dataset está pronto para análises ou se exigiria tratamento prévio.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    A base de dados é estruturalmente íntegra e confiável. Não há ocorrência de registros duplicados ou de valores nulos. Contudo, para garantir análises mais robustas, faz-se necessária a padronização da coluna **sale_date**.

    Quanto à distribuição da coluna **total**, é preciso levar em conta que se trata de uma métrica agregada, composta pelo produto entre valor unitário e quantidade, logo, a presença de valores discrepantes aqui é irrelevante.

    Ao agruparmos as vendas por data, no entanto, foram detectados **outliers** no limite superior que justificam uma investigação mais profunda para identificar se tais eventos representam picos reais de demanda.
    """)
    return


@app.cell
def _(mo, vendas):
    _df = mo.sql(
        f"""
        -- Outliers (Valor Unitário de Produto)
        WITH quartis AS (
            SELECT 
                percentile_cont(0.25) WITHIN GROUP (ORDER BY total) as q1,
                percentile_cont(0.75) WITHIN GROUP (ORDER BY total) as q3
            FROM vendas
        ),
        limites AS (
            SELECT 
                q1, 
                q3, 
                (q3 - q1) as iqr,
                (q1 - 1.5 * (iqr)) as limite_inferior,
                (q3 + 1.5 * (iqr)) as limite_superior
            FROM quartis
        )
        SELECT t.*
        FROM vendas t, limites lm
        WHERE (t.total < lm.limite_inferior OR t.total > lm.limite_superior) AND QTD = 1
        ORDER BY total DESC;
        """
    )
    return


@app.cell
def _(mo, vendas):
    apurado_dia = mo.sql(
        f"""
        -- Agregado de vandas por data
        SELECT SUM(total) AS apurado, sale_date FROM vendas
        GROUP BY sale_date;
        """
    )
    return (apurado_dia,)


@app.cell
def outliers_data(apurado_dia, mo):
    _df = mo.sql(
        f"""
        -- Outliers (Vendas por data)
        WITH quartis AS (
            SELECT 
                percentile_cont(0.25) WITHIN GROUP (ORDER BY apurado) as q1,
                percentile_cont(0.75) WITHIN GROUP (ORDER BY apurado) as q3
            FROM apurado_dia
        ),
        limites AS (
            SELECT 
                q1, 
                q3, 
                (q3 - q1) as iqr,
                (q1 - 1.5 * (iqr)) as limite_inferior,
                (q3 + 1.5 * (iqr)) as limite_superior
            FROM quartis
        )
        SELECT t.*
        FROM apurado_dia t, limites lm
        WHERE (t.apurado < lm.limite_inferior OR t.apurado > lm.limite_superior)
        ORDER BY apurado DESC;
        """
    )
    return


@app.cell
def _(mo, vendas):
    _df = mo.sql(
        f"""
        -- Número de linhas duplicadas
        SELECT 
            (SELECT COUNT(*) FROM vendas) - 
            (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM vendas)) AS total_de_linhas_duplicadas;
        """
    )
    return


@app.cell
def _(mo, vendas):
    _df = mo.sql(
        f"""
        -- Busca por valores nulos
        SELECT * FROM vendas WHERE (COLUMNS(*)) IS NULL;
        """
    )
    return


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Datas mal formatadas
        SELECT sale_date, COUNT(*)
        FROM df
        WHERE TRY_CAST(sale_date AS DATE) IS NULL 
          AND sale_date IS NOT NULL
        GROUP BY ALL;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Q2: Produtos**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q2.1: Python**
    Realize as tarefas a seguir usando exclusivamente Python.

    - Padronize os nomes das categorias de produtos em: eletrônicos, propulsão e ancoragem.
    - Converta os valores para o tipo numérico.
    - Remova as duplicatas.
    """)
    return


@app.cell
def _(carregar_dados):
    produtos = carregar_dados('data/produtos_raw.csv', 'csv')
    produtos.head()
    return (produtos,)


@app.cell
def _(produtos):
    # Padroniza categorias
    def padronizar_categoria(texto):
        # Converte para minúsculo e elimina os espaços em branco
        texto_limpo = texto.lower().replace(' ', '')

        # Classificação simplificada
        if 'elet' in texto_limpo:
            return 'eletrônicos'
        elif 'prop' in texto_limpo:
            return 'propulsão'
        elif 'anc' or 'enc' in texto_limpo:
            return 'ancoragem'
        else:
            return 'outros'

    produtos['actual_category'] = produtos['actual_category'].apply(padronizar_categoria)
    return


@app.cell
def _(produtos):
    # Converte para número
    def converter_para_numero(valor):
        # Remove o cifrão e espaços em branco
        valor_limpo = valor.replace('R$', '').strip()
    
        # Converte para float
        return float(valor_limpo)

    produtos['price'] = produtos['price'].apply(converter_para_numero)
    return


@app.cell
def _(produtos):
    # Remove produtos duplicados
    produtos_limpo = produtos.drop_duplicates(keep='first').reset_index(drop=True)
    produtos_limpo
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q2.2: Validação**
    Quantos produtos duplicados foram removidos?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    Foram removidos **7** produtos duplicados, restando **150** produtos na base.
    """)
    return


@app.cell
def _(produtos):
    # Quantidade de produtos duplicados removidos
    int(produtos.duplicated().sum())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Q3: Custos de Importação**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q3.1: De JSON para o pandas**
    Converta o arquivo `custos_importacao.json` para um DataFrame.
    """)
    return


@app.cell
def _(carregar_dados):
    # Lê arquivo json como um DataFrame pandas
    custos_desnormalizado = carregar_dados('data/custos_importacao.json', 'json')
    custos_desnormalizado
    return (custos_desnormalizado,)


@app.cell
def _(custos_desnormalizado, pd):
    # Normaliza coluna `historic_data` 
    def normalizar_json(df):
        df_explodido = df.explode('historic_data').reset_index(drop=True)
        df_normalizado = pd.json_normalize(df_explodido['historic_data'])
        df_final = pd.concat([df_explodido.drop(columns=['historic_data']), df_normalizado], axis=1)

        return df_final

    custos = normalizar_json(custos_desnormalizado)
    return (custos,)


@app.cell
def _(custos, pd):
    # Converte coluna `start_date` para `datetime`
    custos['start_date'] = pd.to_datetime(custos['start_date'], format='%d/%m/%Y')
    custos.head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q3.2: Validação**
    Quantas entradas de importação o CSV recebeu ao todo após a normalização?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    Após a normalização da coluna `historic_data`, a tabela `custos` contém **1260** registros..
    """)
    return


app._unparsable_cell(
    r"""
    -- Número de linhas do df "custos"
    custos.shape[0]
    """,
    name="_"
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Q4: Dados Públicos**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q4.1: Cálculo e modelagem (SQL)**
    - Calcule o custo total em BRL por transação (custo USD * câmbio do dia)
    - Identifique transações com prejuízo
    - Agregue os dados por `id_produto`, gerando:
        - Receita total (BRL)
        - Prejuízo total (BRL)
        - Percentual de perda (prejuízo_total / receita_total)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q4.2: Análise visual e validação**
    Gere um gráfico que represente o prejuízo total por produto, considerando apenas produtos que tiveram prejuízo.

    Qual é o `id_produto` que apresentou a maior porcentagem de perda financeira relativa (maior % de prejuízo sobre sua receita) no período analisado?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q4.3: Explique sobre o desenvolvimento**
    - Qual data de câmbio você utilizou?
    - Como definiu o prejuízo?
    - Alguma suposição relevante?
    """)
    return


if __name__ == "__main__":
    app.run()
