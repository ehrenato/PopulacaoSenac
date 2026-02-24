"""
Módulo responsável por carregar os dados da camada prata
em um banco de dados SQLite para análise SQL.
"""

from pathlib import Path
import sqlite3
import pandas as pd


# ============================================================
# CONFIGURAÇÕES
# ============================================================

DB_PATH = Path("database/analytics.db")
PRATA_PATH = Path("data/prata")


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def get_connection() -> sqlite3.Connection:
    """
    Cria (ou reutiliza) conexão com o banco SQLite.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def load_table(
    conn: sqlite3.Connection,
    table_name: str,
    df: pd.DataFrame
) -> None:
    """
    Carrega um DataFrame em uma tabela do SQLite.
    Substitui a tabela caso já exista (reprocessamento seguro).
    """
    df.to_sql(
        name=table_name,
        con=conn,
        if_exists="replace",
        index=False
    )

    print(f"Tabela '{table_name}' carregada com sucesso.")


# ============================================================
# CARGA DAS TABELAS
# ============================================================

def run_load_sqlite() -> None:
    """
    Executa a carga da camada prata no banco SQLite.
    """
    print("Iniciando carga dos dados no SQLite...")

    conn = get_connection()

    try:
        estados = pd.read_parquet(PRATA_PATH / "estados.parquet")
        municipios = pd.read_parquet(PRATA_PATH / "municipios.parquet")
        populacao = pd.read_parquet(PRATA_PATH / "populacao.parquet")

        load_table(conn, "estados", estados)
        load_table(conn, "municipios", municipios)
        load_table(conn, "populacao", populacao)

    finally:
        conn.close()

    print("Carga no SQLite finalizada com sucesso.")