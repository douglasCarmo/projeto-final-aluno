from pyspark.sql.functions import round as spark_round, sum as spark_sum

from comum import (
    caminho,
    criar_sessao,
    exigir_camada,
    titulo,
)


def processar_relatorio() -> None:
    """
    Gera o Relatório Executivo consolidado do Data Lake:
    - Exibe os principais eventos e visitantes do Funil de Conversão.
    - Consolida o volume total de vendas e a receita financeira acumulada.
    - Apresenta a auditoria de qualidade (contagem de eventos rejeitados na Silver).
    """
    titulo("INICIANDO GERAÇÃO DO RELATÓRIO EXECUTIVO")

    spark = criar_sessao("relatorio")

    try:
        # 1. Valida dependência da tabela de funil na camada Gold
        exigir_camada(
            spark,
            caminho("gold/funil"),
            "python pipeline/gold.py",
        )

        print("📊 Lendo dados da camada Gold para consolidação do relatório...\n")

        # 2. Métrica: Funil de Conversão (Top 10 eventos)
        df_funil = spark.read.parquet(caminho("gold/funil"))
        print("--- FUNIL DE CONVERSÃO (Top Eventos por Visitantes Únicos) ---")
        df_funil.show(10, truncate=False)

        # 3. Métrica: Total de Vendas e Receita Acumulada
        df_receita = spark.read.parquet(caminho("gold/receita"))
        resumo_financeiro = df_receita.select(
            spark_sum("total_vendas").alias("vendas_totais"),
            spark_round(spark_sum("receita_total"), 2).alias("receita_total_brl"),
        ).collect()[0]

        vendas_totais = resumo_financeiro["vendas_totais"] or 0
        receita_total = resumo_financeiro["receita_total_brl"] or 0.0

        print("--- RESUMO FINANCEIRO (CAMADA GOLD) ---")
        print(f"Total de Vendas Concluídas: {vendas_totais}")
        print(f"Receita Total Acumulada: R$ {receita_total:,.2f}\n")

        # 4. Métrica: Auditoria e Qualidade de Dados (Silver)
        try:
            df_rejeitados = spark.read.parquet(caminho("silver/eventos_rejeitados"))
            qtd_rejeitados = df_rejeitados.count()
        except Exception:
            qtd_rejeitados = 0

        print("--- AUDITORIA E QUALIDADE DE DADOS ---")
        print(f"Eventos Invalidados/Rejeitados na Silver: {qtd_rejeitados}\n")

        print("✔ Relatório executivo gerado com sucesso!")

    finally:
        spark.stop()


if __name__ == "__main__":
    processar_relatorio()