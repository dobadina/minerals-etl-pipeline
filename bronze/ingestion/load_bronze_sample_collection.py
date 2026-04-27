import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema
from utils.logging_utils import get_logger

SOURCE_FILE = "data/source/minerals_appsheet_data_model.xlsx"
SHEET_NAME = "Sample Collection Table"
TABLE_NAME = "bronze.sample_collection"

def load():
    logger = get_logger("bronze")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "bronze")

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
                collection_acceptance_datetime VARCHAR,
                mine_gate_photo VARCHAR,
                mine_gate_photo_url VARCHAR,
                sample_photo VARCHAR,
                sample_photo_url VARCHAR,
                collection_date VARCHAR,
                collection_status VARCHAR,
                collection_status_updated_at VARCHAR,
                driver_id VARCHAR,
                dispatch_date VARCHAR,
                dispatch_status VARCHAR,
                dispatch_status_updated_at VARCHAR,
                bot_record_id VARCHAR
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = pd.read_excel(SOURCE_FILE, sheet_name=SHEET_NAME, dtype=str)

        df.columns = [
            "collection_id", "task_id", "request_id", "sampler_id",
            "sampler_email", "supplier_id", "supplier_phone", "pickup_location_id",
            "collection_acceptance_status", "collection_acceptance_datetime",
            "mine_gate_photo", "mine_gate_photo_url", "sample_photo",
            "sample_photo_url", "collection_date", "collection_status",
            "collection_status_updated_at", "driver_id", "dispatch_date",
            "dispatch_status", "dispatch_status_updated_at", "bot_record_id"
        ]

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