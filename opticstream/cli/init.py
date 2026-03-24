from cyclopts import App
from .root import app

init_cli = app.command(App(name="init"))


@init_cli.command
def blocks():
    from ..config.psoct_scan_config import PSOCTScanConfig
    from ..config.lsm_scan_config import LSMScanConfig

    PSOCTScanConfig.register_type_and_schema()
    LSMScanConfig.register_type_and_schema()

    print("Blocks registered successfully")


import os
import psycopg2
from psycopg2 import sql


DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

TARGET_DB = os.getenv("STATE_DB_NAME", "opticstream_state")
TARGET_TABLE = os.getenv("STATE_TABLE_NAME", "project_state")


def create_database_if_not_exists() -> None:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname="postgres",  # connect to default admin DB first
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (TARGET_DB,),
            )
            exists = cur.fetchone() is not None

            if exists:
                print(f"Database '{TARGET_DB}' already exists.")
            else:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(TARGET_DB)
                    )
                )
                print(f"Created database '{TARGET_DB}'.")
    finally:
        conn.close()


def create_table_if_not_exists() -> None:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=TARGET_DB,
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        project_type text NOT NULL,
                        project_name text NOT NULL,
                        state jsonb NOT NULL,
                        created_at timestamptz NOT NULL DEFAULT now(),
                        updated_at timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (project_type, project_name)
                    )
                    """
                ).format(sql.Identifier(TARGET_TABLE))
            )
            print(f"Ensured table '{TARGET_TABLE}' exists in '{TARGET_DB}'.")
    finally:
        conn.close()


def main() -> None:
    create_database_if_not_exists()
    create_table_if_not_exists()
    print("Done.")

