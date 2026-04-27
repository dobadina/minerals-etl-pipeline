import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection
from utils.logging_utils import get_logger

SOURCE_FILE = "data/source/minerals_appsheet_data_model.xlsx"

TABLES_IN_SCOPE = {
    "bronze.sampling_requests": "Sampling Requests Table",
    "bronze.task_assignment": "Task Assignment Table",
    "bronze.sample_collection": "Sample Collection Table",
    "bronze.lab_test_results": "Lab Test Results Table",
    "bronze.supplier": "Supplier Table",
    "bronze.samplers": "Samplers Table",
    "bronze.mineral": "Mineral Table",
    "bronze.locations": "Locations Table"
}

def validate():
    logger = get_logger("bronze")
    logger.info("========================================")
    logger.info("Bronze validation started")
    logger.info("========================================")

    conn = get_connection()
    all_passed = True

    for table_name, sheet_name in TABLES_IN_SCOPE.items():
        source_df = pd.read_excel(SOURCE_FILE, sheet_name=sheet_name, dtype=str)
        source_count = len(source_df)

        bronze_count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        if source_count == bronze_count:
            logger.info(f"PASS | {table_name} | source: {source_count} | bronze: {bronze_count}")
        else:
            logger.error(f"FAIL | {table_name} | source: {source_count} | bronze: {bronze_count}")
            all_passed = False

    conn.close()

    logger.info("========================================")
    if all_passed:
        logger.info("Bronze validation completed | all checks passed")
    else:
        logger.error("Bronze validation completed | some checks failed")
    logger.info("========================================")

    return all_passed

if __name__ == "__main__":
    validate()