import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema, standardise_phone
from utils.logging_utils import get_logger

TABLE_NAME = "silver.sampling_requests"

def load():
    logger = get_logger("silver")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "silver")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                request_id VARCHAR,
                scm_email VARCHAR,
                scm_name VARCHAR,
                mineral_type_id VARCHAR,
                test_type_id VARCHAR,
                supplier_id VARCHAR,
                supplier_phone VARCHAR,
                pickup_location_id VARCHAR,
                state VARCHAR,
                region VARCHAR,
                created_at DATE,
                request_status VARCHAR,
                status_updated_at DATE,
                comments VARCHAR,
                next_step VARCHAR,
                ingestion_at TIMESTAMP
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = conn.execute("""
            SELECT * FROM bronze.sampling_requests
        """).df()

        df['scm_email'] = df['scm_email'].str.strip().str.lower()
        df['scm_name'] = df['scm_name'].str.strip()
        df['state'] = df['state'].str.strip()
        df['region'] = df['region'].str.strip()
        df['request_status'] = df['request_status'].str.strip()
        df['next_step'] = df['next_step'].str.strip()
        df['comments'] = df['comments'].apply(
            lambda x: None if pd.isna(x) or '#ERROR' in str(x) else str(x).strip()
        )
        df['supplier_phone'] = df['supplier_phone'].apply(
            lambda x: standardise_phone(str(x)) if pd.notna(x) else None
        )
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce').dt.date
        df['status_updated_at'] = pd.to_datetime(
            df['status_updated_at'], errors='coerce'
        ).dt.date
        df['ingestion_at'] = datetime.now()

        df = df[[
            'request_id', 'scm_email', 'scm_name', 'mineral_type_id',
            'test_type_id', 'supplier_id', 'supplier_phone', 'pickup_location_id',
            'state', 'region', 'created_at', 'request_status', 'status_updated_at',
            'comments', 'next_step', 'ingestion_at'
        ]]

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