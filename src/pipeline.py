#EXTRAÇÃO → BRONZE
#TRANSFORMAÇÃO → PRATA
#CONSOLIDAÇÃO → OURO
#CARGA → BANCO ANALÍTICO

from datetime import datetime

from src.extract.ibge_api import run_extraction
from src.transform.transform_data import run_transformation
from src.transform.build_gold import run_gold
from src.load.load_sqlite import run_load


def run_pipeline() -> None:
    """
    Executa o pipeline completo de dados:
    - Extração
    - Transformação
    - Carga no banco analítico
    """
    start_time = datetime.now()
    print("=" * 60)
    print("INICIANDO PIPELINE DE DADOS - DESAFIO SENAC RN")
    print(f"Início: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        print("\n[1/3] Extração de dados")
        run_extraction()

        print("\n[2/3] Transformação de dados")
        run_transformation()

        print("\n[2/3] Consolidando dados")
        run_gold()

        print("\n[3/3] Carga no banco analítico")
        run_load()

        end_time = datetime.now()
        duration = end_time - start_time

        print("\n" + "=" * 60)
        print("PIPELINE FINALIZADO COM SUCESSO ✅")
        print(f"Fim: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duração total: {duration}")
        print("=" * 60)

    except Exception as error:
        print("\n" + "=" * 60)
        print("ERRO NA EXECUÇÃO DO PIPELINE ❌")
        print(f"Detalhes: {error}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    run_pipeline()