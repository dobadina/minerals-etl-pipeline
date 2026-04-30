import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema, standardise_phone
from utils.logging_utils import get_logger

TABLE_NAME = "silver.sample_collection"

def load():
    logger = get_logger("silver")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "silver")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                collection_id VARCHAR,
                task_id VARCHAR,
                request_id VARCHAR,
                sampler_id VARCHAR,
                sampler_email VARCHAR,
                supplier_id VARCHAR,
                supplier_phone VARCHAR,
                pickup_location_id VARCHAR,
                collection_acceptance_status VARCHAR,
                collection_acceptance_datetime TIMESTAMP,
                collection_date DATE,
                collection_status VARCHAR,
                driver_id VARCHAR,
                dispatch_date DATE,
                dispatch_status VARCHAR,
                bot_record_id VARCHAR,
                ingestion_at TIMESTAMP
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = conn.execute("""
            SELECT * FROM bronze.sample_collection
        """).df()

        df['sampler_email'] = df['sampler_email'].str.strip().str.lower()
        df['collection_status'] = df['collection_status'].str.strip()
        df['dispatch_status'] = df['dispatch_status'].str.strip()

        # Fix collection_acceptance_status — replace datetime values with NULL
        valid_statuses = ['Accepted', 'Declined']
        df['collection_acceptance_status'] = df['collection_acceptance_status'].apply(
            lambda x: x if x in valid_statuses else None
        )

        # Fix supplier_phone — replace errors with NULL
        df['supplier_phone'] = df['supplier_phone'].apply(
            lambda x: standardise_phone(str(x)) if pd.notna(x) else None
        )

        df['collection_acceptance_datetime'] = pd.to_datetime(
            df['collection_acceptance_datetime'], errors='coerce'
        )
        df['collection_date'] = pd.to_datetime(
            df['collection_date'], errors='coerce'
        ).dt.date
        df['dispatch_date'] = pd.to_datetime(
            df['dispatch_date'], errors='coerce'
        ).dt.date
        df['ingestion_at'] = datetime.now()

        df = df[[
            'collection_id', 'task_id', 'request_id', 'sampler_id',
            'sampler_email', 'supplier_id', 'supplier_phone', 'pickup_location_id',
            'collection_acceptance_status', 'collection_acceptance_datetime',
            'collection_date', 'collection_status', 'driver_id',
            'dispatch_date', 'dispatch_status', 'bot_record_id', 'ingestion_at'
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