import json
from datetime import datetime, timezone

from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    countDistinct,
    lit,
    sum as spark_sum,
)

from comum import (
    arquivo_do_projeto,
    caminho,
    criar_sessao,
    exigir_camada,
    glob_eventos,
    ler_eventos,
    titulo,
)


def processar_gold() -> None:
    """
    Processa a Camada Gold do Data Lake:
    1. Valida dependência da camada Silver (R2).
    2. Consolida a tabela de Funil de Conversão (gold/funil).
    3. Consolida a receita diária de vendas (gold/receita).
    4. Gera métricas agregadas de cliques e rolagem para o Mapa de Calor.
    5. Exporta o resultado para site/mapa_calor.js (R11).
    """
    titulo("INICIANDO PROCESSAMENTO: CAMADA GOLD")

    spark = criar_sessao("gold")

    try:
        # 1. Requisito R2: Garante existência da camada Silver
        exigir_camada(
            spark,
            glob_eventos("silver"),
            "python pipeline/silver.py",
        )

        print("Lendo dados validados da camada Silver...")
        df_silver = ler_eventos(spark, camada="silver")

        cols = df_silver.columns
        col_visitor = (
            "visitor_id"
            if "visitor_id" in cols
            else ("user_pseudo_id" if "user_pseudo_id" in cols else "user_id")
        )

        # Inspeciona campos internos de 'params' com segurança
        params_fields = []
        if "params" in cols and hasattr(df_silver.schema["params"].dataType, "fieldNames"):
            params_fields = df_silver.schema["params"].dataType.fieldNames()

        # -------------------------------------------------------------------------
        # Tabela Gold 1: Funil de Conversão (gold/funil)
        # -------------------------------------------------------------------------
        df_funil = (
            df_silver.groupBy("event_name")
            .agg(
                countDistinct(col_visitor).alias("visitantes_unicos"),
                count("event_id").alias("total_eventos"),
            )
            .orderBy(col("visitantes_unicos").desc())
        )

        path_funil = caminho("gold/funil")
        df_funil.write.mode("overwrite").parquet(path_funil)
        print(f"✔ Tabela Gold Funil gravada em: {path_funil}")

        # -------------------------------------------------------------------------
        # Tabela Gold 2: Receita Diária (gold/receita)
        # -------------------------------------------------------------------------
        df_purchases = df_silver.filter(col("event_name") == "purchase")

        if "value" in params_fields:
            col_valor = col("params.value")
        elif "value" in cols:
            col_valor = col("value")
        else:
            col_valor = lit(0.0)

        col_valor_limpo = coalesce(col_valor.cast("double"), lit(0.0))

        df_receita = (
            df_purchases.groupBy("year", "month", "day")
            .agg(
                count("event_id").alias("total_vendas"),
                spark_sum(col_valor_limpo).alias("receita_total"),
            )
            .orderBy("year", "month", "day")
        )

        path_receita = caminho("gold/receita")
        df_receita.write.mode("overwrite").parquet(path_receita)
        print(f"✔ Tabela Gold Receita gravada em: {path_receita}")

        # -------------------------------------------------------------------------
        # Tabela Gold 3 & 4: Mapa de Calor (Cliques e Rolagem)
        # -------------------------------------------------------------------------
        # Mapa de Cliques por elemento da página
        if "element_id" in params_fields:
            col_element = coalesce(col("params.element_id"), lit("desconhecido"))
        elif "element_id" in cols:
            col_element = coalesce(col("element_id"), lit("desconhecido"))
        else:
            col_element = lit("desconhecido")

        df_cliques = (
            df_silver.filter(col("event_name") == "click")
            .select(col_element.alias("element_id"), col("page"))
            .groupBy("element_id", "page")
            .agg(count("*").alias("total_cliques"))
        )

        path_cliques = caminho("gold/mapa_calor_cliques")
        df_cliques.write.mode("overwrite").parquet(path_cliques)
        print(f"✔ Tabela Gold Mapa de Calor (Cliques) gravada em: {path_cliques}")

        # Mapa de Rolagem (percent_scrolled / depth)
        if "percent_scrolled" in params_fields:
            col_depth = coalesce(col("params.percent_scrolled"), lit(0))
        elif "depth" in params_fields:
            col_depth = coalesce(col("params.depth"), lit(0))
        elif "depth" in cols:
            col_depth = coalesce(col("depth"), lit(0))
        else:
            col_depth = lit(0)

        df_rolagem = (
            df_silver.filter(col("event_name") == "scroll")
            .select(col_depth.alias("depth"), col("page"))
            .groupBy("depth", "page")
            .agg(count("*").alias("total_ocorrencias"))
        )

        path_rolagem = caminho("gold/mapa_calor_rolagem")
        df_rolagem.write.mode("overwrite").parquet(path_rolagem)
        print(f"✔ Tabela Gold Mapa de Calor (Rolagem) gravada em: {path_rolagem}")

        # -------------------------------------------------------------------------
        # Requisito R11: Exportação local em JS para a Landing Page
        # -------------------------------------------------------------------------
        print("Exportando dados do mapa de calor para site/mapa_calor.js (R11)...")
        cliques_list = [row.asDict() for row in spark.read.parquet(path_cliques).collect()]
        rolagem_list = [row.asDict() for row in spark.read.parquet(path_rolagem).collect()]

        agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        dados_js = {
            "gerado_em": agora,
            "cliques": cliques_list,
            "rolagem": rolagem_list,
        }

        destino_js = arquivo_do_projeto("site/mapa_calor.js")
        conteudo = f"window.MAPA_CALOR = {json.dumps(dados_js, ensure_ascii=False, default=float)};\n"
        destino_js.write_text(conteudo, encoding="utf-8")
        print(f"✔ Arquivo local atualizado: {destino_js}")

        print("✔ Camada Gold concluída com sucesso!")

    finally:
        spark.stop()


if __name__ == "__main__":
    processar_gold()