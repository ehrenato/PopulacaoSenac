"""
Geração da camada ouro a partir do banco SQLite,
utilizando consultas SQL analíticas.
"""

from pathlib import Path
import sqlite3
import pandas as pd

DB_PATH = Path("database/analytics.db")
OURO_PATH = Path("data/ouro")
SQL_PATH = Path("sql")

OURO_PATH.mkdir(parents=True, exist_ok=True)


def execute_query(query_file: str, output_file: str) -> None:
    """
    Executa uma query SQL e salva o resultado em Parquet (camada ouro).
    """
    with sqlite3.connect(DB_PATH) as conn:
        query = (SQL_PATH / query_file).read_text(encoding="utf-8")
        df = pd.read_sql_query(query, conn)

    df.to_parquet(OURO_PATH / output_file, index=False)
    print(f"Arquivo ouro gerado: {output_file}")


def run_gold_generation() -> None:
    """
    Executa todas as consultas analíticas e gera a camada ouro.
    """
    print("Iniciando geração da camada ouro...")

    execute_query("01_top_10_municipios.sql", "top_10_municipios.parquet")
    execute_query("02_populacao_por_estado.sql", "populacao_por_estado.parquet")
    execute_query("03_media_populacao_estado.sql", "media_populacao_estado.parquet")
    execute_query("04_ranking_regioes.sql", "ranking_regioes.parquet")

    print("Camada ouro gerada com sucesso.")