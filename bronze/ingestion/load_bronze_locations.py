import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema
from utils.logging_utils import get_logger

SOURCE_FILE = "data/source/minerals_appsheet_data_model.xlsx"
SHEET_NAME = "Locations Table"
TABLE_NAME = "bronze.locations"

def load():
    logger = get_logger("bronze")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "bronze")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                location_id VARCHAR,
                state_name VARCHAR,
                region_name VARCHAR
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        df = pd.read_excel(SOURCE_FILE, sheet_name=SHEET_NAME, dtype=str)

        df.columns = ["location_id", "state_name", "region_name"]

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