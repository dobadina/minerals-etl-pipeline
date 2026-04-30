import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema, standardise_phone
from utils.logging_utils import get_logger

TABLE_NAME = "silver.task_assignment"

def load():
    logger = get_logger("silver")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "silver")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                task_id VARCHAR,
                request_id VARCHAR,
                mineral_type_id VARCHAR,
                supplier_id VARCHAR,
                supplier_phone VARCHAR,
                pickup_location VARCHAR,
                state VARCHAR,
                region VARCHAR,
                assigned_sampler_id VARCHAR,
                lab_id VARCHAR,
                task_status VARCHAR,
                sampler_acceptance_status VARCHAR,
                status_updated_at DATE,
                assigned_by VARCHAR,
                assigned_at DATE,
                request_created_date DATE,
                ingestion_at TIMESTAMP
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = conn.execute("""
            SELECT * FROM bronze.task_assignment
        """).df()

        df['supplier_phone'] = df['supplier_phone'].apply(
            lambda x: standardise_phone(str(x)) if pd.notna(x) else None
        )
        df['pickup_location'] = df['pickup_location'].str.strip()
        df['state'] = df['state'].str.strip()
        df['region'] = df['region'].str.strip()
        df['task_status'] = df['task_status'].str.strip()
        df['sampler_acceptance_status'] = df['sampler_acceptance_status'].apply(
            lambda x: None if pd.isna(x) or str(x).strip() == 'None' else str(x).strip()
        )
        df['status_updated_at'] = pd.to_datetime(
            df['status_updated_at'], errors='coerce', format='mixed'
        ).dt.date
        df['assigned_at'] = pd.to_datetime(
            df['assigned_at'], errors='coerce', format='mixed'
        ).dt.date
        df['request_created_date'] = pd.to_datetime(
            df['request_created_date'], errors='coerce', format='mixed'
        ).dt.date
        df['ingestion_at'] = datetime.now()

        df = df[[
            'task_id', 'request_id', 'mineral_type_id', 'supplier_id',
            'supplier_phone', 'pickup_location', 'state', 'region',
            'assigned_sampler_id', 'lab_id', 'task_status',
            'sampler_acceptance_status', 'status_updated_at', 'assigned_by',
            'assigned_at', 'request_created_date', 'ingestion_at'
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