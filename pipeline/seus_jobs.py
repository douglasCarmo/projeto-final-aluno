import sys
from pathlib import Path

# Garante a importação correta dos módulos locais
sys.path.append(str(Path(__file__).parent))

from bronze import processar_bronze
from silver import processar_silver
from gold import processar_gold


def main() -> None:
    """Executa a sequência completa do Data Lake (Bronze -> Silver -> Gold)."""
    # 1. Processa Bronze (Lê landing/ e gera bronze/)
    processar_bronze()

    # 2. Processa Silver (Lê bronze/ e gera silver/)
    processar_silver()

    # 3. Processa Gold (Lê silver/, gera gold/ e exporta site/mapa_calor.js)
    processar_gold()


if __name__ == "__main__":
    main()