import sqlite3
import pandas as pd
from pathlib import Path


PRATA_PATH = Path("data/prata")
DB_PATH = Path("database/analytics.db")


def load_parquet(filename: str) -> pd.DataFrame:
    """
    Carrega um arquivo Parquet da camada prata.
    """
    return pd.read_parquet(PRATA_PATH / filename)


def create_tables(conn: sqlite3.Connection) -> None:
    """
    Cria as tabelas analíticas no banco SQLite.
    """
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS estados (
            id_estado INTEGER PRIMARY KEY,
            nome_estado TEXT,
            sigla TEXT,
            regiao TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS municipios (
            id_municipio INTEGER PRIMARY KEY,
            nome_municipio TEXT,
            id_estado INTEGER,
            FOREIGN KEY (id_estado) REFERENCES estados (id_estado)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS populacao (
            id_municipio INTEGER,
            populacao INTEGER,
            ano INTEGER,
            PRIMARY KEY (id_municipio, ano),
            FOREIGN KEY (id_municipio) REFERENCES municipios (id_municipio)
        )
    """)

    conn.commit()


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    conn: sqlite3.Connection
) -> None:
    """
    Carrega um DataFrame no banco SQLite substituindo dados existentes.
    """
    df.to_sql(
        table_name,
        conn,
        if_exists="replace",
        index=False
    )
    print(f"Tabela '{table_name}' carregada com sucesso.")


def run_load() -> None:
    """
    Executa o processo completo de carga no banco analítico.
    """
    print("Iniciando carga no banco SQLite...")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    create_tables(conn)

    estados = load_parquet("estados.parquet")
    municipios = load_parquet("municipios.parquet")
    populacao = load_parquet("populacao.parquet")

    load_dataframe(estados, "estados", conn)
    load_dataframe(municipios, "municipios", conn)
    load_dataframe(populacao, "populacao", conn)

    conn.close()
    print("Carga finalizada com sucesso.")