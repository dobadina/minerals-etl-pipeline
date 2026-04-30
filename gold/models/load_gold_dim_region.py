from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema
from utils.logging_utils import get_logger

TABLE_NAME = "gold.dim_region"

def load():
    logger = get_logger("gold")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "gold")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                region_key INTEGER,
                location_id VARCHAR,
                state_name VARCHAR,
                region_name VARCHAR
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        conn.execute(f"""
            INSERT INTO {TABLE_NAME}
            SELECT
                ROW_NUMBER() OVER (ORDER BY location_id) AS region_key,
                location_id,
                state_name,
                region_name
            FROM silver.locations
        """)

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