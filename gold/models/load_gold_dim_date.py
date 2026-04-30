import pandas as pd
from datetime import datetime, date
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema
from utils.logging_utils import get_logger

TABLE_NAME = "gold.dim_date"

def load():
    logger = get_logger("gold")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "gold")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                date_key INTEGER,
                full_date DATE,
                day INTEGER,
                month INTEGER,
                month_name VARCHAR,
                quarter INTEGER,
                year INTEGER,
                day_of_week INTEGER,
                day_name VARCHAR,
                is_weekend BOOLEAN
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        # Get min and max dates from silver
        result = conn.execute("""
            SELECT
                MIN(created_at) as min_date,
                MAX(created_at) as max_date
            FROM silver.sampling_requests
        """).fetchone()

        min_date = result[0]
        max_date = result[1]

        logger.info(f"Generating date dimension from {min_date} to {max_date}")

        date_range = pd.date_range(start=min_date, end=max_date, freq='D')

        rows = []
        for d in date_range:
            rows.append({
                'date_key': int(d.strftime('%Y%m%d')),
                'full_date': d.date(),
                'day': d.day,
                'month': d.month,
                'month_name': d.strftime('%B'),
                'quarter': (d.month - 1) // 3 + 1,
                'year': d.year,
                'day_of_week': d.dayofweek + 1,
                'day_name': d.strftime('%A'),
                'is_weekend': d.dayofweek >= 5
            })

        df = pd.DataFrame(rows)
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