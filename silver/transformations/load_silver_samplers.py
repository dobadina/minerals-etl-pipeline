import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema, standardise_phone
from utils.logging_utils import get_logger

TABLE_NAME = "silver.samplers"

def load():
    logger = get_logger("silver")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "silver")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                sampler_id VARCHAR,
                full_name VARCHAR,
                phone_number VARCHAR,
                state_assigned VARCHAR,
                region_assigned VARCHAR,
                email VARCHAR,
                ingestion_at TIMESTAMP
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = conn.execute("""
            SELECT * FROM bronze.samplers
        """).df()

        df['full_name'] = df['full_name'].str.strip().str.title()
        df['phone_number'] = df['phone_number'].apply(
            lambda x: standardise_phone(str(x)) if pd.notna(x) else None
        )
        df['state_assigned'] = df['state_assigned'].str.strip()
        df['region_assigned'] = None
        df['email'] = df['email'].str.strip().str.lower()
        df['ingestion_at'] = datetime.now()

        conn.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM df")

        row_count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        duration = (datetime.now() - start_time).seconds

        logger.info(f"Completed: {TABLE_NAME} | rows: {row_count} | duration: {duration}s")
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Failed: {TABLE_NAME} | error: {str(e)}")
        return False

if __name__ == "__main__":
    load()