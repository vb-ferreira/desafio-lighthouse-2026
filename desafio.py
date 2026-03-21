import marimo

__generated_with = "0.21.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import altair as alt
    from sklearn.metrics import mean_absolute_error
    from sklearn.metrics.pairwise import cosine_similarity

    return alt, cosine_similarity, mean_absolute_error, mo, pd


@app.cell
def _(pd):
    def carregar_dados(arquivo):
        tipo = arquivo.split('.')[1].lower().strip()
    
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
    vendas = carregar_dados('data/vendas_2023_2024.csv')
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
def _(pd, vendas):
    # Converte coluna `sale_date` para `datetime`
    vendas['sale_date'] = pd.to_datetime(vendas['sale_date'], format='mixed')
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
        -- Agregado de vendas por data
        -- Output: aprado_dia
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
def _(alt, apurado_dia):
    boxplot = alt.Chart(apurado_dia).mark_boxplot(size=80).encode(
        y=alt.Y('apurado:Q', title='Valor Apurado (BRL)'),
        tooltip=[
            alt.Tooltip('sale_date:N', title='Data da Venda'),
            alt.Tooltip('apurado:Q', title='Valor Apurado', format=',.2f')
        ]
    ).properties(
        title=alt.TitleParams(
            text='Distribuição do valor apurado por data',
        ),
        width=300, 
        height=400
    )

    boxplot
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
def _(mo, vendas):
    _df = mo.sql(
        f"""
        -- Datas mal formatadas
        SELECT sale_date, COUNT(*)
        FROM vendas
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
    produtos = carregar_dados('data/produtos_raw.csv')
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
        if 'R$' in valor:
            valor_limpo = valor.replace('R$', '').strip()
        else:
            valor_limpo = valor.replace(',', '.').strip()

        # Converte para float
        return float(valor_limpo)

    produtos['price'] = produtos['price'].apply(converter_para_numero)
    return (converter_para_numero,)


@app.cell
def _(produtos):
    # Remove produtos duplicados
    produtos_limpo = produtos.drop_duplicates(keep='first').reset_index(drop=True)
    return (produtos_limpo,)


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
    custos_desnormalizado = carregar_dados('data/custos_importacao.json')
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

    Após a normalização da coluna `historic_data`, a tabela `custos` contém **1260** registros.
    """)
    return


@app.cell
def _(custos):
    # Número de linhas do df "custos"
    custos.shape[0]
    return


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


@app.cell
def _(custos, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM custos;
        """
    )
    return


@app.cell
def _(mo, vendas):
    _df = mo.sql(
        f"""
        SELECT * FROM vendas LIMIT 1;
        """
    )
    return


@app.cell
def _(custos, mo, vendas):
    custo_vendas = mo.sql(
        f"""
        -- Junção das tabelas de custos e vendas (considerando históricos)
        -- Output: custos_venda
        WITH custos_com_fim AS (
            SELECT 
                *,
                -- Cria intervalo de "preço vigente"
                LEAD(start_date) OVER (PARTITION BY product_id ORDER BY start_date) AS end_date
            FROM custos
        )
        -- Juntar onde a venda cai dentro desse intervalo
        SELECT v.id_client, v.id_product, v.qtd, v.total, v.sale_date, (c.usd_price * v.qtd) AS usd_price
        FROM vendas v
        LEFT JOIN custos_com_fim c 
            ON v.id_product = c.product_id
           AND v.sale_date >= c.start_date 
           AND (v.sale_date < c.end_date OR c.end_date IS NULL);
        """
    )
    return (custo_vendas,)


@app.cell
def _(carregar_dados):
    # Carrega cotação do dolar no período
    dolar = carregar_dados('data/dolar_2023_2024.csv', 'csv')
    return (dolar,)


@app.cell
def _(converter_para_numero, dolar, pd):
    # Converte coluna `dataHoraCotacao` para `datetime` e `cotacaoCompra` para float
    dolar['dataHoraCotacao'] = pd.to_datetime(dolar['dataHoraCotacao'], format='mixed')
    dolar['cotacaoCompra'] = dolar['cotacaoCompra'].apply(converter_para_numero)
    return


@app.cell
def _(custo_vendas, dolar, mo):
    receita_menos_custo = mo.sql(
        f"""
        -- Calcula lucro/prejuízo das transações
        -- Output: receita_menos_custo
        SELECT 
            v.id_client, 
            v.id_product, 
            v.qtd, 
            v.total AS receita_transacao, 
            v.sale_date,
            d.cotacaoCompra AS dolar_dia,
            (v.usd_price * v.qtd) AS custo_transacao_usd,
            (v.usd_price * dolar_dia) AS custo_transacao_brl,
            (receita_transacao - custo_transacao_brl) AS diff_receita_custo
        FROM custo_vendas v
        LEFT JOIN dolar d 
            ON v.sale_date = d.dataHoraCotacao;
        """
    )
    return (receita_menos_custo,)


@app.cell
def _(mo, receita_menos_custo):
    _df = mo.sql(
        f"""
        -- Seleciona transações com prejuízo
        SELECT * FROM receita_menos_custo
        WHERE diff_receita_custo < 0;
        """
    )
    return


@app.cell
def _(mo, receita_menos_custo):
    prejuizo_por_produto = mo.sql(
        f"""
        -- Prejuízo por produto
        -- Output: prejuizo_por_produto
        SELECT 
            id_product AS id_produto,
        	ROUND(SUM(receita_transacao), 2) AS receita_brl,
            ROUND(SUM(diff_receita_custo), 2) AS prejuizo_brl,
            ROUND((prejuizo_brl / receita_brl), 2) AS perda_percentual
        FROM receita_menos_custo
        GROUP BY id_product
        ORDER BY perda_percentual ASC;
        """
    )
    return (prejuizo_por_produto,)


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
    /// admonition | Resposta:

    O produto de id **72** foi o que apresentou maior porcentagem de perda financeira relativa no período (**63%**).
    """)
    return


@app.cell
def _(alt, prejuizo_por_produto):
    # Gráfico do prejuízo por produto (10 maiores)
    df_prejuizo = prejuizo_por_produto[prejuizo_por_produto['perda_percentual'] < 0].copy()
    df_prejuizo_top10 = df_prejuizo.sort_values(by='perda_percentual', ascending=True).head(10)
    df_prejuizo_top10['perda_percentual_positiva'] = df_prejuizo_top10['perda_percentual'].abs()

    chart = alt.Chart(df_prejuizo_top10).mark_bar().encode(
        x=alt.X('perda_percentual_positiva:Q', title='Perda Percentual', axis=alt.Axis(format='%')),
        y=alt.Y('id_produto:N', title='ID do Produto', sort='-x'),
        tooltip=['id_produto', 'perda_percentual', 'prejuizo_brl', 'receita_brl']
    ).properties(
        title=alt.TitleParams(
            text='10 produtos com maiores prejuízos relativos',
            subtitle='2023 - 2024'
        ),
        width=600,
        height=400
    ).configure_title(
        fontSize=18,
        subtitleFontSize=13,
        subtitleColor='gray',
        offset=10
    )

    chart
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    Utilizei a média diária do valor de compra do dólar comercial, extraída do site do Banco Central.

    O prejuízo da transação (em BRL) foi definido pelo produto `(custo_usd * quantidade * cotacao_dolar)`.

    Usando esta métrica, 121 dos 150 produtos apresentaram prejuízo acumulado no período.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Q5: Análise de clientes**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q5.1: Código SQL**
    Código calculando:
    - O Ticket Médio e a Diversidade de categorias por cliente.
    - A identificação e filtro dos 10 clientes "Fiéis" (maior Ticket Médio entre aqueles com diversidade >= 3 categorias).
    - A categoria mais vendida (em quantidade total de itens) considerando apenas o histórico desses 10 clientes.
    """)
    return


@app.cell
def _(mo, vendas):
    _df = mo.sql(
        f"""
        SELECT * FROM vendas;
        """
    )
    return


@app.cell
def _(mo, produtos_limpo):
    _df = mo.sql(
        f"""
        SELECT * FROM produtos_limpo;
        """
    )
    return


@app.cell
def _(mo, produtos_limpo, vendas):
    vendas_categoria = mo.sql(
        f"""
        -- Junção das tabelas vendas e produtos
        -- Output: vendas_categoria
        SELECT
            v.id AS id_venda,
            v.id_client AS id_cliente,
            v.id_product AS id_produto,
            p.actual_category AS categoria,
            p.name AS descricao,
            v.qtd,
            v.total
        FROM vendas v
        LEFT JOIN produtos_limpo p
            ON v.id_product = p.code;
        """
    )
    return (vendas_categoria,)


@app.cell
def _(mo, vendas_categoria):
    faturamento_cliente = mo.sql(
        f"""
        -- Calcula faturamento total, frequência, ticket médio e diversidade (por cliente)
        -- Output: faturamento_cliente
        SELECT 
            id_cliente,
            ROUND(SUM(total), 2) AS faturamento_total,
            COUNT(id_venda) AS frequencia,
            ROUND((faturamento_total / frequencia), 2) AS ticket_medio,
            COUNT(DISTINCT categoria) AS diversidade
        FROM vendas_categoria
        GROUP BY id_cliente
        ORDER BY ticket_medio DESC;
        """
    )
    return (faturamento_cliente,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q5.2: Validação**
    Considerando apenas as compras realizadas pelos Top 10 Clientes selecionados (Critério: Maior Ticket Médio com 3+ categorias): Qual foi a categoria de produtos mais vendida para eles (maior quantidade total de itens)?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    A categoria com mais itens vendidos para os 10 cliente selecionados é a de **propulsão**, com **6030** itens.
    """)
    return


@app.cell
def _(faturamento_cliente, mo, vendas_categoria):
    _categoria_mais_vendida = mo.sql(
        f"""
        -- Usa a tabela faturamento_cliente para filtrar os 10 clientes com maior ticket_medio
        -- Output: categoria_mais_vendida
        WITH top_10_clientes AS (
            SELECT 
                id_cliente
            FROM faturamento_cliente
            LIMIT 10
        )
        SELECT 
            categoria,
            SUM(qtd) AS total_itens_vendidos
        FROM vendas_categoria
        WHERE id_cliente IN (SELECT id_cliente FROM top_10_clientes)
        GROUP BY categoria
        ORDER BY total_itens_vendidos DESC
        LIMIT 1;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q5.3: Explique sobre o desenvolvimento**
    - Como você realizou a limpeza das categorias?
    - Qual lógica utilizou para filtrar os clientes com diversidade mínima?
    - Como garantiu que a contagem de itens refletisse apenas os Top 10?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    As categorias já haviam sido padronizadas ao responder a **Questão 2**. Usamos uma **estrutura condicional** e o método `apply()` do **pandas**, para aplicar a função à coluna.


    ```python
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
    ```

    Não foi necessário criar filtro de diversidade mínima, uma vez que todos os clientes compraram produtos de ao menos uma das três categorias.

    Usamos uma **CTE (Common Table Expression)** para selecionar os 10 clientes com maior `ticket_médio`, e aplicamos essa seleção como filtro na consulta de agregação por categoria para descobrir a quantidade vendida em cada uma delas.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Q6: Dimensão de calendário**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q6.1: Código SQL**
    Código com:
    - Desenvolvimento de um calendário com os dias da semana (em portugues)
    - LEFT JOIN entre o calendário e a tabela de vendas
    - agregação de vendas por dia (soma de valor_venda),
    - substituição de valores nulos por zero para dias sem vendas
    """)
    return


@app.cell
def _(mo):
    calendario_dim = mo.sql(
        f"""
        -- Dimensão de datas
        -- Output: calendario_dim
        WITH base AS (
            SELECT CAST(range AS DATE) AS data_pura
            FROM range('2023-01-01'::DATE, '2025-01-01'::DATE, INTERVAL '1 day')
        )
        SELECT 
            CAST(data_pura AS DATETIME) AS data,
            CASE dayofweek(data_pura)
                WHEN 0 THEN 'Domingo'
                WHEN 1 THEN 'Segunda-feira'
                WHEN 2 THEN 'Terça-feira'
                WHEN 3 THEN 'Quarta-feira'
                WHEN 4 THEN 'Quinta-feira'
                WHEN 5 THEN 'Sexta-feira'
                WHEN 6 THEN 'Sábado'
            END AS dia_semana
        FROM base;
        """
    )
    return (calendario_dim,)


@app.cell
def _(calendario_dim, mo, vendas):
    vendas_completa = mo.sql(
        f"""
        -- Junção das tabelas venda e calendario
        -- Output: vendas_completa
        SELECT 
            v.id_client AS id_cliente,
            v.id_product AS id_produto,
            COALESCE(v.qtd, 0) AS qtd,
            COALESCE(v.total, 0) AS total,
            c.data,
            c.dia_semana
        FROM calendario_dim c
        LEFT JOIN vendas v
        	ON c.data = v.sale_date;
        """
    )
    return (vendas_completa,)


@app.cell
def _(mo, vendas_completa):
    _df = mo.sql(
        f"""
        -- Calcula média e total de vendas por dia da semana
        SELECT
            dia_semana,
            SUM(total) AS total,
            ROUND(AVG(total), 2) AS media
        FROM vendas_completa
        GROUP BY dia_semana
        ORDER BY total ASC;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q6.2: Validação**
    Após considerar os dias zerados no cálculo: Qual é o Dia da Semana (ex: Domingo, Segunda...) que apresenta a menor média de vendas histórica, e qual é o valor dessa média arredondada para 2 casas decimais?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    **Segunda-feira** é o dia com menor média de vendas históricas: **R$ 246.800,74**.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q6.3: Explique**
    - Por que é necessário utilizar uma tabela de datas (calendário) em vez de agrupar diretamente a tabela de vendas?
    - O que aconteceria com a média de vendas se um dia da semana tivesse muitos dias sem nenhuma venda registrada?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    A tabela de vendas não contém informações sobre todos os dias do período.

    Ao não considerarmos esses dias ausentes - e atribuirmos "zero" a eles - a **média** pode ser **inflada artificialmente**.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Q7: Previsão de demanda**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q7.1: Código em python**
    Adicione o código python usado para construção do modelo
    """)
    return


@app.cell
def _(mo, produtos_limpo, vendas_completa):
    df_final = mo.sql(
        f"""
        -- Junta tabelas vendas e produtos
        -- Output: df_final
        SELECT
        	v.id_cliente,
            v.id_produto,
            p.name AS descricao,
            p.actual_category AS categoria,
            v.qtd,
            v.total,
            v.data,
            v.dia_semana
        FROM vendas_completa v
        LEFT JOIN produtos_limpo p
        	ON v.id_produto = p.code;
        """
    )
    return (df_final,)


@app.cell
def _(df_final):
    # Filtra o produto específico e ordena pela data da transação
    df_motor = df_final[df_final['descricao'] == "Motor de Popa Yamaha Evo Dash 155HP"].copy().sort_values('data')
    return (df_motor,)


@app.cell
def _(df_motor, pd):
    # Agrupa o total de vendas por dia
    vendas_diarias = df_motor.groupby('data')['total'].sum().reset_index()

    # Cria calendário completo, preenchendo com zero dias sem vendas
    data_inicio = pd.to_datetime('2023-01-01')
    data_fim = pd.to_datetime('2024-01-31')
    calendario = pd.DataFrame({'data': pd.date_range(start=data_inicio, end=data_fim, freq='D')})
    df_completo = pd.merge(calendario, vendas_diarias, on='data', how='left').fillna(0)
    return (df_completo,)


@app.cell
def _(df_completo, mean_absolute_error):
    # Construção do Baseline - Média Móvel de 7 dias
    df_completo['mm_7'] = df_completo['total'].shift(1).rolling(window=7).mean()

    # Dados de teste
    filtro_teste = (df_completo['data'] >= '2024-01-01') & (df_completo['data'] <= '2024-01-31')
    df_teste = df_completo[filtro_teste].copy()

    # Cálculo do MAE
    mae = mean_absolute_error(df_teste['total'], df_teste['mm_7'])

    # Previsão de faturamento da primeira semana (01/01 a 07/01)
    filtro_prim_sem = (df_teste['data'] >= '2024-01-01') & (df_teste['data'] <= '2024-01-07')
    soma_receita = df_teste[filtro_prim_sem]['mm_7'].sum()

    print(f"MAE: {mae:.2f}")
    print(f"Previsão total de faturamento na 1ª semana de Janneiro de 2024: {round(soma_receita)}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q7.2: Validação**
    Utilizando seu modelo treinado, qual é a soma total da previsão de vendas (arredondada para número inteiro) para o 'Motor de Popa Yamaha Evo Dash 155HP' durante a primeira semana de Janeiro de 2024 (01/01 a 07/01)?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    **O faturamento previsto na primeira semana é 0**. Como não houve nenhuma venda desse item específico nos últimos 7 dias de dezembro de 2023, a média móvel calculada para a primeira semana de janeiro resultou em zero.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q7.3: Explique**
    - Como o baseline foi construído?
    - Como evitou data leakage?
    - Uma limitação do modelo proposto.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    O modelo calculou a previsão para um determinado dia fazendo a **média aritmética simples** da receita nos **7 dias imediatamente anteriores**.

    O vazamento de dados (**data leakage**) foi evitado usando uma função de deslocamento temporal (`shift(1)`) antes de calcular a média. Assim, a previsão para 01/01/2024, por exemplo, não tem acesso aos dados reais desse dia, apenas aos dados até 31/12/2023.

    Este **modelo é limitado capturar tendências e sazonalidades**. Ele assume que o comportamento da próxima semana será igual ao da semana passada, ignorando que as vendas podem flutuar dependendo do mês/período do ano.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Q8: Sistema de recomendação**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q8.1: Código em python**
    Script em python que:
    - Constrói a matriz usuário–item
    - Calcula a similaridade de cosseno
    - Gera o ranking de similaridade
    """)
    return


@app.cell
def _(df_final):
    # Remove linhas com valores nulos nas variáveis de interesse 
    df_recomenda = df_final.dropna(subset=['id_cliente', 'id_produto', 'descricao'])

    # Identifica id do produto de interesse
    produto_alvo = "GPS Garmin Vortex Maré Drift"
    id_alvo = df_recomenda[df_recomenda['descricao'] == produto_alvo]['id_produto'].unique()[0]
    return df_recomenda, id_alvo


@app.cell
def _(df_recomenda):
    # Cria a Matriz Usuário x Produto
    interacoes = df_recomenda[['id_cliente', 'id_produto']].drop_duplicates()
    interacoes['comprou'] = 1

    matriz_usuario_produto = interacoes.pivot(
        index='id_cliente', 
        columns='id_produto', 
        values='comprou'
    ).fillna(0)

    matriz_usuario_produto
    return (matriz_usuario_produto,)


@app.cell
def _(cosine_similarity, matriz_usuario_produto, pd):
    # Cálcula similaridade do cosseno
    similaridade_cos = cosine_similarity(matriz_usuario_produto.T)

    df_similaridade = pd.DataFrame(
        similaridade_cos, 
        index=matriz_usuario_produto.columns, 
        columns=matriz_usuario_produto.columns
    ).round(4)

    df_similaridade
    return (df_similaridade,)


@app.cell
def _(df_similaridade, id_alvo):
    # Classifica produtos por similaridade
    ranking = df_similaridade[id_alvo].sort_values(ascending=False)

    # Desconsideraa a similaridade do produto com ele mesmo (que sempre será 1.0)
    ranking = ranking.drop(id_alvo)

    # Obter o Top 5
    top_5_ids = ranking.head(5)

    top_5_ids
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q8.2: Validação**
    Qual é o id_produto com MAIOR similaridade ao “GPS Garmin Vortex Maré Drift”?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    O produto de maior similaridade é o "**Motor de Popa Volvo Magnum 276HP**" (`id_produto = 94`), com um score de similaridade de aproximadamente **0.8696**.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Q8.3: Explique**
    - Como a matriz foi construída?
    - O que significa a similaridade de cosseno nesse contexto?
    - Uma limitação desse método de recomendação.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    /// admonition | Resposta:

    A **matriz** foi construída transformando os dados em uma estrutura onde cada linha representa um cliente único (`id_cliente`) e cada coluna representa um produto único (`id_produto`). Primeiro, filtramos a base, removendo duplicatas (desconsiderarando a quantidade de vezes que um cliente comprou o mesmo produto). Em seguida, preenchemos os cruzamentos com o valor 1 se o cliente comprou o produto e 0 caso caso contrário.

    Nesse contexto, a **similaridade de cosseno** mede o quão parecidos são os clientes que compram dois produtos diferentes.

    Uma limitação clássica desse tipo de modelo é o "**Cold Start**". Ao ser cadastrado, um **novo produto não terá histórico de compras**. Logo, sua similaridade com qualquer outro produto na matriz será 0.
    """)
    return


if __name__ == "__main__":
    app.run()
