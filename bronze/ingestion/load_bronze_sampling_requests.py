import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema
from utils.logging_utils import get_logger

SOURCE_FILE = "data/source/minerals_appsheet_data_model.xlsx"
SHEET_NAME = "Sampling Requests Table"
TABLE_NAME = "bronze.sampling_requests"

def load():
    logger = get_logger("bronze")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "bronze")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                scm_email VARCHAR,
                scm_name VARCHAR,
                request_id VARCHAR,
                mineral_type_id VARCHAR,
                test_type_id VARCHAR,
                supplier_id VARCHAR,
                supplier_phone VARCHAR,
                pickup_location_id VARCHAR,
                state VARCHAR,
                region VARCHAR,
                created_at VARCHAR,
                request_status VARCHAR,
                status_updated_at VARCHAR,
                comments VARCHAR,
                next_step VARCHAR
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = pd.read_excel(SOURCE_FILE, sheet_name=SHEET_NAME, dtype=str)

        df.columns = [
            "scm_email", "scm_name", "request_id", "mineral_type_id",
            "test_type_id", "supplier_id", "supplier_phone",
            "pickup_location_id", "state", "region", "created_at",
            "request_status", "status_updated_at", "comments", "next_step"
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