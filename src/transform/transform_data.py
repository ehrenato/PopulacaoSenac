"""
Módulo responsável pela transformação dos dados
da camada bronze para a camada prata.
"""

from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def extract_id_estado(microrregiao: Optional[Dict[str, Any]]) -> Optional[int]:
    """
    Extrai o id do estado a partir da estrutura de microrregião.
    Retorna None caso a estrutura esteja ausente ou incompleta.
    """
    if not isinstance(microrregiao, dict):
        return None

    try:
        return microrregiao["mesorregiao"]["UF"]["id"]
    except KeyError:
        return None


def get_latest_partition(entity: str) -> Path:
    """
    Retorna o caminho da partição mais recente da camada bronze
    para uma determinada entidade.
    """
    base_path = Path("data/bronze") / entity

    partitions = sorted(base_path.glob("dt_extracao=*"))
    if not partitions:
        raise FileNotFoundError(f"Nenhuma partição encontrada para {entity}")

    return partitions[-1]


# ============================================================
# TRANSFORMAÇÕES
# ============================================================

def transform_estados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma dados brutos de estados.
    """
    estados = df.copy()

    estados = estados.rename(
        columns={
            "id": "id_estado",
            "nome": "nome_estado",
            "sigla": "sigla"
        }
    )

    estados["regiao"] = estados["regiao"].apply(
        lambda x: x.get("nome") if isinstance(x, dict) else None
    )

    return estados[["id_estado", "nome_estado", "sigla", "regiao"]]


def transform_municipios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma dados brutos de municípios.
    """
    municipios = df.copy()

    municipios["id_estado"] = municipios["microrregiao"].apply(extract_id_estado)

    municipios = municipios.dropna(subset=["id_estado"])

    municipios = municipios.rename(
        columns={
            "id": "id_municipio",
            "nome": "nome_municipio"
        }
    )

    return municipios[["id_municipio", "nome_municipio", "id_estado"]]


def transform_populacao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma dados de população.
    """
    populacao = df.copy()

    populacao["id_municipio"] = populacao["id_municipio"].astype(int)
    populacao["populacao"] = populacao["populacao"].astype(int)
    populacao["ano"] = populacao["ano"].astype(int)

    return populacao[["id_municipio", "populacao", "ano"]]


# ============================================================
# ORQUESTRAÇÃO DA TRANSFORMAÇÃO
# ============================================================

def run_transformation() -> None:
    """
    Executa a transformação da camada bronze para prata.
    """
    print("Iniciando transformação dos dados...")

    # ESTADOS
    estados_path = get_latest_partition("estados") / "estados.parquet"
    estados_raw = pd.read_parquet(estados_path)
    estados = transform_estados(estados_raw)
    estados.to_parquet("data/prata/estados.parquet", index=False)

    # MUNICÍPIOS
    municipios_path = get_latest_partition("municipios") / "municipios.parquet"
    municipios_raw = pd.read_parquet(municipios_path)
    municipios = transform_municipios(municipios_raw)
    municipios.to_parquet("data/prata/municipios.parquet", index=False)

    # POPULAÇÃO
    populacao_path = get_latest_partition("populacao") / "populacao_2022.parquet"
    populacao_raw = pd.read_parquet(populacao_path)
    populacao = transform_populacao(populacao_raw)
    populacao.to_parquet("data/prata/populacao.parquet", index=False)

    print("Transformação concluída com sucesso.")