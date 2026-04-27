import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema
from utils.logging_utils import get_logger

SOURCE_FILE = "data/source/minerals_appsheet_data_model.xlsx"
SHEET_NAME = "Lab Test Results Table"
TABLE_NAME = "bronze.lab_test_results"

def load():
    logger = get_logger("bronze")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "bronze")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                test_result_id VARCHAR,
                collection_id VARCHAR,
                task_id VARCHAR,
                request_id VARCHAR,
                sampler_id VARCHAR,
                driver_id VARCHAR,
                result_document VARCHAR,
                result_document_url VARCHAR,
                uploaded_by VARCHAR,
                uploader_name VARCHAR,
                result_received_date VARCHAR,
                analysis_status VARCHAR,
                status_updated_at VARCHAR,
                was_sample_received VARCHAR
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = pd.read_excel(SOURCE_FILE, sheet_name=SHEET_NAME, dtype=str)

        df.columns = [
            "test_result_id", "collection_id", "task_id", "request_id",
            "sampler_id", "driver_id", "result_document", "result_document_url",
            "uploaded_by", "uploader_name", "result_received_date",
            "analysis_status", "status_updated_at", "was_sample_received"
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