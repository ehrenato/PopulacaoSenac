import pandas as pd
from pathlib import Path


BRONZE_PATH = Path("data/bronze")
PRATA_PATH = Path("data/prata")


def load_parquet(filename: str) -> pd.DataFrame:
    """
    Carrega um arquivo Parquet da camada bronze.

    Args:
        filename (str): Nome do arquivo.

    Returns:
        pd.DataFrame: DataFrame carregado.
    """
    return pd.read_parquet(BRONZE_PATH / filename)


def save_parquet(df: pd.DataFrame, filename: str) -> None:
    """
    Salva um DataFrame em formato Parquet na camada prata.

    Args:
        df (pd.DataFrame): DataFrame a ser salvo.
        filename (str): Nome do arquivo.
    """
    PRATA_PATH.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PRATA_PATH / filename, index=False)
    print(f"Arquivo transformado salvo em data/prata/{filename}")


def transform_estados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma os dados brutos de estados.

    Returns:
        pd.DataFrame: Estados normalizados.
    """
    estados = df[["id", "nome", "sigla", "regiao"]].copy()

    estados["regiao"] = estados["regiao"].apply(lambda x: x["nome"])
    estados.rename(columns={
        "id": "id_estado",
        "nome": "nome_estado"
    }, inplace=True)

    return estados.drop_duplicates()


def transform_municipios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma os dados brutos de municípios.

    Returns:
        pd.DataFrame: Municípios normalizados.
    """
    municipios = df[["id", "nome", "microrregiao"]].copy()

    municipios["id_estado"] = municipios["microrregiao"].apply(
        lambda x: x["mesorregiao"]["UF"]["id"]
    )

    municipios.rename(columns={
        "id": "id_municipio",
        "nome": "nome_municipio"
    }, inplace=True)

    return municipios[["id_municipio", "nome_municipio", "id_estado"]].drop_duplicates()


def transform_populacao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma os dados de população.

    Returns:
        pd.DataFrame: População por município e ano.
    """
    return df[["id_municipio", "populacao", "ano"]].drop_duplicates()


def run_transformation() -> None:
    """
    Executa o processo completo de transformação dos dados.
    """
    print("Iniciando transformação dos dados...")

    estados_raw = load_parquet("estados.parquet")
    municipios_raw = load_parquet("municipios.parquet")
    populacao_raw = load_parquet("populacao_2022.parquet")

    estados = transform_estados(estados_raw)
    municipios = transform_municipios(municipios_raw)
    populacao = transform_populacao(populacao_raw)

    save_parquet(estados, "estados.parquet")
    save_parquet(municipios, "municipios.parquet")
    save_parquet(populacao, "populacao.parquet")

    print("Transformação finalizada com sucesso.")