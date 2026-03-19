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
    def load_data():
        return pd.read_csv('data/vendas_2023_2024.csv')

    return (load_data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Q1: EDA**
    """)
    return


@app.cell
def _(load_data):
    df = load_data()
    df.head()
    return (df,)


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
def profiling(mo):
    _df = mo.sql(
        f"""
        -- Profiling com função exclusiva do DuckDB
        SUMMARIZE 'df';
        """
    )
    return


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Quantidade de linhas
        SELECT COUNT(*) FROM df;
        """
    )
    return


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Quantidade e tipo de dados de cada coluna
        DESCRIBE df;
        """
    )
    return


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Mínimos, Máximos e Média (total)
        SELECT 
          MIN(sale_date) AS primeira_data, 
          MAX(sale_date) AS ultima_data, 
          MIN(total) AS total_minimo, 
          MAX(total) AS total_macimo, 
          ROUND(AVG(total), 2) AS total_media
        FROM df;
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
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Valor máximo da coluna total
        SELECT MAX(total) FROM df;
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
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Outliers (Valor Unitário de Produto)
        WITH quartis AS (
            SELECT 
                percentile_cont(0.25) WITHIN GROUP (ORDER BY total) as q1,
                percentile_cont(0.75) WITHIN GROUP (ORDER BY total) as q3
            FROM df
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
        FROM df t, limites lm
        WHERE (t.total < lm.limite_inferior OR t.total > lm.limite_superior) AND QTD = 1
        ORDER BY total DESC;
        """
    )
    return


@app.cell
def _(df, mo):
    apurado_dia = mo.sql(
        f"""
        -- Agregado de vandas por data
        SELECT SUM(total) AS apurado, sale_date FROM df
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
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Número de linhas duplicadas
        SELECT 
            (SELECT COUNT(*) FROM df) - 
            (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM df)) AS total_de_linhas_duplicadas;
        """
    )
    return


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        -- Busca por valores nulos
        SELECT * FROM df WHERE (COLUMNS(*)) IS NULL;
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
 
    """)
    return


if __name__ == "__main__":
    app.run()
