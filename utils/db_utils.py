import duckdb
import os

DB_PATH = "data/minerals_etl.duckdb"

def get_connection():
    os.makedirs("data", exist_ok=True)
    return duckdb.connect(DB_PATH)

def create_schema(conn, schema_name):
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")