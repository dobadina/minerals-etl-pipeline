import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db_utils import get_connection
from utils.logging_utils import get_logger

EXPORT_DIR = "data/reporting"

GOLD_TABLES = [
    "dim_supplier",
    "dim_sampler",
    "dim_mineral",
    "dim_region",
    "dim_date",
    "fact_sampling_requests"
]

def export():
    logger = get_logger("gold")
    logger.info("========================================")
    logger.info("Reporting export started")
    logger.info("========================================")
    start_time = datetime.now()

    try:
        os.makedirs(EXPORT_DIR, exist_ok=True)

        conn = get_connection()

        for table in GOLD_TABLES:
            output_path = os.path.join(EXPORT_DIR, f"{table}.csv").replace("\\", "/")
            conn.execute(f"""
                COPY (SELECT * FROM gold.{table})
                TO '{output_path}' (HEADER, DELIMITER ',')
            """)
            row_count = conn.execute(
                f"SELECT COUNT(*) FROM gold.{table}"
            ).fetchone()[0]
            logger.info(f"Exported: {table}.csv | rows: {row_count}")

        conn.close()

        duration = (datetime.now() - start_time).seconds
        logger.info("========================================")
        logger.info(f"Reporting export completed | duration: {duration}s")
        logger.info(f"Export location: {EXPORT_DIR}")
        logger.info("========================================")
        return True

    except Exception as e:
        logger.error(f"Reporting export failed | error: {str(e)}")
        return False

if __name__ == "__main__":
    export()