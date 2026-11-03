import sys
from pathlib import Path

# Garante a importação correta dos módulos locais
sys.path.append(str(Path(__file__).parent))

from bronze import processar_bronze
from gold import processar_gold
from relatorio import processar_relatorio
from silver import processar_silver


def main() -> None:
    """Executa a sequência completa do Data Lake (Bronze -> Silver -> Gold -> Relatório)."""
    # 1. Camada Bronze: Ingestão fiel dos dados brutos com metadados
    processar_bronze()

    # 2. Camada Silver: Limpeza, qualidade, deduplicação e tabela de itens
    processar_silver()

    # 3. Camada Gold: Consolidação de métricas (Funil, Receita, Mapa de Calor) e exportação R11
    processar_gold()

    # 4. Relatório Executivo: Exibição dos indicadores finais no console/terminal
    processar_relatorio()


if __name__ == "__main__":
    main()