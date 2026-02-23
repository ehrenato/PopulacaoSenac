import requests
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List


BASE_URL = "https://servicodados.ibge.gov.br/api"
BRONZE_PATH = Path("data/bronze")


def fetch_data(endpoint: str) -> List[Dict[str, Any]]:
    """
    Realiza uma requisição GET para a API do IBGE.

    Args:
        endpoint (str): Endpoint da API (sem a base URL).

    Returns:
        List[Dict[str, Any]]: Dados retornados pela API em formato JSON.

    Raises:
        RuntimeError: Caso ocorra erro na requisição.
    """
    url = f"{BASE_URL}{endpoint}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as error:
        raise RuntimeError(f"Erro ao acessar {url}: {error}") from error


def extract_estados() -> pd.DataFrame:
    """
    Extrai a lista de estados do IBGE.

    Returns:
        pd.DataFrame: DataFrame contendo os estados.
    """
    data = fetch_data("/v1/localidades/estados")
    return pd.DataFrame(data)


def extract_municipios() -> pd.DataFrame:
    """
    Extrai a lista de municípios do IBGE.

    Returns:
        pd.DataFrame: DataFrame contendo os municípios.
    """
    data = fetch_data("/v1/localidades/municipios")
    return pd.DataFrame(data)


def extract_populacao_2022() -> pd.DataFrame:
    """
    Extrai a população por município (Censo 2022).

    Returns:
        pd.DataFrame: DataFrame com população por município.
    """
    data = fetch_data(
        "/v3/agregados/4714/periodos/2022/variaveis/93?localidades=N6[all]"
    )

    registros = data[0]["resultados"][0]["series"]

    rows = []
    for item in registros:
        rows.append({
            "id_municipio": int(item["localidade"]["id"]),
            "populacao": int(item["serie"]["2022"]),
            "ano": 2022
        })

    return pd.DataFrame(rows)


def save_parquet(df: pd.DataFrame, filename: str) -> None:
    """
    Cria diretório da camada bronze.
    Salva um DataFrame em formato Parquet na camada bronze.

    Args:
        df (pd.DataFrame): DataFrame a ser salvo.
        filename (str): Nome do arquivo.
    """
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)
    file_path = BRONZE_PATH / filename
    df.to_parquet(file_path, index=False)
    print(f"Arquivo salvo em {file_path}")


def run_extraction() -> None:
    """
    Executa o processo completo de extração de dados.
    """
    print("Iniciando extração de dados do IBGE...")

    estados = extract_estados()
    save_parquet(estados, "estados.parquet")

    municipios = extract_municipios()
    save_parquet(municipios, "municipios.parquet")

    populacao = extract_populacao_2022()
    save_parquet(populacao, "populacao_2022.parquet")

    print("Extração finalizada com sucesso.")