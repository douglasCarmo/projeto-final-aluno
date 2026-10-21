import sys
from pathlib import Path

# Garante que as importações do diretório 'pipeline' funcionem corretamente
sys.path.append(str(Path(__file__).parent))

from bronze import processar_bronze


def main() -> None:
    """Ponto de entrada para rodar as camadas do Data Lake."""
    # Executa a camada Bronze
    processar_bronze()

    # Próximas camadas serão chamadas aqui no futuro:
    # processar_silver()
    # processar_gold()


if __name__ == "__main__":
    main()