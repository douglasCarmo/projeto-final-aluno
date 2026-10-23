from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    dayofmonth,
    explode_outer,
    format_string,
    lit,
    month,
    row_number,
    to_timestamp,
    year,
)
from pyspark.sql.window import Window

from comum import (
    caminho,
    criar_sessao,
    exigir_camada,
    glob_eventos,
    gravar_eventos,
    ler_eventos,
    titulo,
)


def processar_silver() -> None:
    """
    Processa a Camada Silver do Data Lake:
    - Identifica dinamicamente as colunas do schema (event_ts vs timestamp / user_pseudo_id vs visitor_id).
    - Converte datas/timestamps com segurança.
    - Aplica regras de qualidade de dados e segrega eventos rejeitados.
    - Deduplica eventos por event_id.
    - Normaliza a tabela de itens/produtos (silver/itens).
    - Grava os eventos validados na raiz da Silver com overwrite dinâmico.
    """
    titulo("INICIANDO PROCESSAMENTO: CAMADA SILVER")

    spark = criar_sessao("silver")

    try:
        # 1. Requisito R2: Garante a existência da camada Bronze
        exigir_camada(
            spark,
            glob_eventos("bronze"),
            "python pipeline/bronze.py",
        )

        print("Lendo eventos da camada Bronze...")
        df_bronze = ler_eventos(spark, camada="bronze")

        cols = df_bronze.columns

        # 2. Resolução dinâmica dos nomes de colunas conforme o contrato
        col_ts = "event_ts" if "event_ts" in cols else "timestamp"
        col_visitor = (
            "user_pseudo_id"
            if "user_pseudo_id" in cols
            else ("visitor_id" if "visitor_id" in cols else "user_id")
        )

        # 3. Conversão segura de timestamp (trata strings ISO e Unix epoch em milissegundos)
        df_com_ts = df_bronze.withColumn(
            "dt_evento",
            coalesce(
                to_timestamp(col(col_ts)),
                (col(col_ts).cast("double") / 1000).cast("timestamp"),
            ),
        )

        # 4. Regras de Qualidade: Validação de campos obrigatórios
        condicao_valido = (
            col("event_id").isNotNull()
            & col(col_visitor).isNotNull()
            & col("dt_evento").isNotNull()
            & col("event_name").isNotNull()
        )

        df_rejeitados = (
            df_com_ts.filter(~condicao_valido)
            .withColumn(
                "_motivo_rejeicao",
                lit(f"Campos obrigatorios nulos (event_id, {col_visitor}, dt_evento ou event_name)"),
            )
            .withColumn("_dh_rejeicao", current_timestamp())
        )

        df_validos = df_com_ts.filter(condicao_valido)

        # 5. Gravação de registros rejeitados em pasta separada
        if df_rejeitados.limit(1).count() > 0:
            path_rejeitados = caminho("silver/eventos_rejeitados")
            df_rejeitados.write.mode("overwrite").parquet(path_rejeitados)
            print(f"⚠ Registos rejeitados gravados em: {path_rejeitados}")

        # 6. Deduplicação por event_id (mantém o mais recente)
        coluna_ordem = "_dh_processamento" if "_dh_processamento" in cols else "dt_evento"
        window_spec = Window.partitionBy("event_id").orderBy(col(coluna_ordem).desc())

        df_deduplicado = (
            df_validos
            .withColumn("_rn", row_number().over(window_spec))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        # 7. Formatação explicita das colunas de partição (ano, mês e dia)
        df_silver_eventos = (
            df_deduplicado
            .withColumn("year", format_string("%04d", year(col("dt_evento"))))
            .withColumn("month", format_string("%02d", month(col("dt_evento"))))
            .withColumn("day", format_string("%02d", dayofmonth(col("dt_evento"))))
        )

        # 8. Extração e normalização de itens/produtos de compra (suporta 'items' ou 'itens')
        col_items = "items" if "items" in cols else ("itens" if "itens" in cols else None)

        if col_items and col_items in df_silver_eventos.columns:
            df_itens_raw = df_silver_eventos.filter(col(col_items).isNotNull())
            if df_itens_raw.limit(1).count() > 0:
                df_itens = (
                    df_itens_raw
                    .select(
                        col("event_id"),
                        col(col_visitor).alias("visitor_id"),
                        col("dt_evento"),
                        col("year"),
                        col("month"),
                        col("day"),
                        explode_outer(col(col_items)).alias("item"),
                    )
                    .select(
                        col("event_id"),
                        col("visitor_id"),
                        col("dt_evento"),
                        col("year"),
                        col("month"),
                        col("day"),
                        col("item.*"),
                    )
                )
                path_itens = caminho("silver/itens")
                (
                    df_itens.write
                    .mode("overwrite")
                    .option("partitionOverwriteMode", "dynamic")
                    .partitionBy("year", "month", "day")
                    .parquet(path_itens)
                )
                print(f"✔ Tabela de itens extraída e gravada em: {path_itens}")

        # 9. Gravação dos eventos validados na camada Silver
        print("Gravando eventos validados na camada Silver...")
        gravar_eventos(df_silver_eventos, camada="silver", modo="overwrite")

        print("✔ Camada Silver concluída com sucesso!")

    finally:
        spark.stop()


if __name__ == "__main__":
    processar_silver()