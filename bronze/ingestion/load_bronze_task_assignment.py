import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema
from utils.logging_utils import get_logger

SOURCE_FILE = "data/source/minerals_appsheet_data_model.xlsx"
SHEET_NAME = "Task Assignment Table"
TABLE_NAME = "bronze.task_assignment"

def load():
    logger = get_logger("bronze")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "bronze")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                task_helper VARCHAR,
                task_id VARCHAR,
                request_id VARCHAR,
                request_created_date VARCHAR,
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
                status_updated_at VARCHAR,
                assigned_by VARCHAR,
                assigned_at VARCHAR
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = pd.read_excel(SOURCE_FILE, sheet_name=SHEET_NAME, dtype=str)

        df.columns = [
            "task_helper", "task_id", "request_id", "request_created_date",
            "mineral_type_id", "supplier_id", "supplier_phone", "pickup_location",
            "state", "region", "assigned_sampler_id", "lab_id", "task_status",
            "sampler_acceptance_status", "status_updated_at", "assigned_by", "assigned_at"
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