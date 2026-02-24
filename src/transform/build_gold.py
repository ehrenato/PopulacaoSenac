import pandas as pd
from pathlib import Path

PRATA_PATH = Path("data/prata")
OURO_PATH = Path("data/ouro")


def load_prata(filename: str) -> pd.DataFrame:
    """Carrega dados da camada prata."""
    return pd.read_parquet(PRATA_PATH / filename)


def save_ouro(df: pd.DataFrame, filename: str) -> None:
    """Salva dados na camada ouro."""
    OURO_PATH.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OURO_PATH / filename, index=False)
    print(f"Camada ouro salva em data/ouro/{filename}")


def build_populacao_municipio() -> pd.DataFrame:
    """
    Cria tabela ouro consolidada de população por município.
    """
    estados = load_prata("estados.parquet")
    municipios = load_prata("municipios.parquet")
    populacao = load_prata("populacao.parquet")

    df = (
        populacao
        .merge(municipios, on="id_municipio", how="inner")
        .merge(estados, on="id_estado", how="inner")
    )

    return df[
        [
            "id_municipio",
            "nome_municipio",
            "id_estado",
            "nome_estado",
            "sigla",
            "regiao",
            "populacao",
            "ano",
        ]
    ]


def run_gold() -> None:
    """Executa a construção da camada ouro."""
    print("Iniciando construção da camada ouro...")

    df_ouro = build_populacao_municipio()
    save_ouro(df_ouro, "populacao_municipio_2022.parquet")

    print("Camada ouro criada com sucesso.")