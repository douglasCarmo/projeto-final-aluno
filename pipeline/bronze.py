from pyspark.sql.functions import current_timestamp, input_file_name

from comum import (
    criar_sessao,
    exigir_camada,
    glob_eventos,
    gravar_eventos,
    ler_eventos,
    titulo,
)


def processar_bronze() -> None:
    """Processa os dados brutos da Landing Zone e grava na Camada Bronze."""
    titulo("INICIANDO PROCESSAMENTO: CAMADA BRONZE")

    # 1. Cria a SparkSession configurada para o Floci (fix do Path Style Access)
    spark = criar_sessao("bronze")

    try:
        # 2. Requisito R2: Verifica se os dados da landing zone existem
        exigir_camada(
            spark,
            glob_eventos("landing"),
            "python gerador/gerar_eventos.py",
        )

        # 3. Requisito R1/R4: Lê os eventos JSON da Landing Zone
        print("Lendo eventos brutos da Landing Zone...")
        df_landing = ler_eventos(spark, camada="landing", formato="json")

        # 4. Adiciona metadados de auditoria mantendo a fidelidade dos dados brutos
        df_bronze = (
            df_landing
            .withColumn("_arquivo_origem", input_file_name())
            .withColumn("_dh_processamento", current_timestamp())
        )

        # 5. Grava os dados em Parquet particionados por event_name/year/month/day
        print("Gravando eventos na camada Bronze em formato Parquet...")
        gravar_eventos(df_bronze, camada="bronze", modo="append")

        print("✔ Camada Bronze concluída com sucesso!")

    finally:
        # Garante o fechamento da sessão Spark
        spark.stop()


if __name__ == "__main__":
    processar_bronze()