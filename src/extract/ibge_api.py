"""
Módulo responsável pela extração de dados da API do IBGE
e persistência dos dados brutos na camada bronze.
"""

from datetime import date
from pathlib import Path
from typing import Any, List

import requests
import pandas as pd


# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

BASE_URL = "https://servicodados.ibge.gov.br"


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def fetch_data(endpoint: str) -> List[Any]:
    """
    Realiza requisição GET para a API do IBGE.

    Args:
        endpoint (str): Endpoint da API.

    Returns:
        List[Any]: Dados retornados pela API em formato JSON.

    Raises:
        RuntimeError: Caso a requisição falhe.
    """
    url = f"{BASE_URL}{endpoint}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"Erro ao acessar API do IBGE: {exc}") from exc


def save_bronze_parquet(
    df: pd.DataFrame,
    entity: str,
    filename: str
) -> None:
    """
    Salva DataFrame na camada bronze no formato Parquet,
    particionado pela data de extração.

    Args:
        df (pd.DataFrame): DataFrame a ser salvo.
        entity (str): Nome da entidade (ex.: estados, municipios).
        filename (str): Nome do arquivo Parquet.
    """
    extraction_date = date.today().isoformat()

    base_path = (
        Path("data")
        / "bronze"
        / entity
        / f"dt_extracao={extraction_date}"
    )

    base_path.mkdir(parents=True, exist_ok=True)

    file_path = base_path / filename
    df.to_parquet(file_path, index=False)

    print(f"Arquivo salvo em {file_path}")


# ============================================================
# EXTRAÇÕES ESPECÍFICAS
# ============================================================

def extract_estados() -> None:
    """
    Extrai a lista de estados do IBGE e salva na camada bronze.
    """
    print("Extraindo estados...")
    data = fetch_data("/api/v1/localidades/estados")
    df = pd.DataFrame(data)

    save_bronze_parquet(
        df=df,
        entity="estados",
        filename="estados.parquet"
    )


def extract_municipios() -> None:
    """
    Extrai a lista de municípios do IBGE e salva na camada bronze.
    """
    print("Extraindo municípios...")
    data = fetch_data("/api/v1/localidades/municipios")
    df = pd.DataFrame(data)

    save_bronze_parquet(
        df=df,
        entity="municipios",
        filename="municipios.parquet"
    )


def extract_populacao_2022() -> None:
    """
    Extrai dados de população por município (Censo 2022)
    e salva na camada bronze.
    """
    print("Extraindo população (Censo 2022)...")

    endpoint = (
        "/api/v3/agregados/4714/periodos/2022/"
        "variaveis/93?localidades=N6[all]"
    )

    data = fetch_data(endpoint)

    # Estrutura específica da API de agregados
    registros = data[0]["resultados"][0]["series"]

    rows = []
    for item in registros:
        rows.append({
            "id_municipio": item["localidade"]["id"],
            "populacao": int(item["serie"]["2022"]),
            "ano": 2022
        })

    df = pd.DataFrame(rows)

    save_bronze_parquet(
        df=df,
        entity="populacao",
        filename="populacao_2022.parquet"
    )


# ============================================================
# FUNÇÃO ORQUESTRADORA DA EXTRAÇÃO
# ============================================================

def run_extraction() -> None:
    """
    Executa todas as extrações de dados do IBGE.
    """
    print("Iniciando extração de dados do IBGE...")

    extract_estados()
    extract_municipios()
    extract_populacao_2022()

    print("Extração finalizada com sucesso.")