import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema
from utils.logging_utils import get_logger

TABLE_NAME = "silver.lab_test_results"

def load():
    logger = get_logger("silver")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "silver")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                test_result_id VARCHAR,
                collection_id VARCHAR,
                task_id VARCHAR,
                request_id VARCHAR,
                sampler_id VARCHAR,
                driver_id VARCHAR,
                uploaded_by VARCHAR,
                result_received_date DATE,
                analysis_status VARCHAR,
                status_updated_at DATE,
                was_sample_received VARCHAR,
                ingestion_at TIMESTAMP
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = conn.execute("""
            SELECT * FROM bronze.lab_test_results
        """).df()

        df['uploaded_by'] = df['uploaded_by'].str.strip().str.lower()
        df['analysis_status'] = df['analysis_status'].apply(
            lambda x: None if pd.isna(x) or str(x).strip() == 'None' else str(x).strip()
        )
        df['was_sample_received'] = df['was_sample_received'].apply(
            lambda x: None if pd.isna(x) or str(x).strip() == 'None' else str(x).strip()
        )
        df['result_received_date'] = pd.to_datetime(
            df['result_received_date'], errors='coerce'
        ).dt.date
        df['status_updated_at'] = pd.to_datetime(
            df['status_updated_at'], errors='coerce'
        ).dt.date
        df['ingestion_at'] = datetime.now()

        df = df[[
            'test_result_id', 'collection_id', 'task_id', 'request_id',
            'sampler_id', 'driver_id', 'uploaded_by', 'result_received_date',
            'analysis_status', 'status_updated_at', 'was_sample_received',
            'ingestion_at'
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