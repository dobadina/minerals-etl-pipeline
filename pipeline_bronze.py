import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logging_utils import get_logger
from bronze.ingestion.load_bronze_sampling_requests import load as load_sampling_requests
from bronze.ingestion.load_bronze_task_assignment import load as load_task_assignment
from bronze.ingestion.load_bronze_sample_collection import load as load_sample_collection
from bronze.ingestion.load_bronze_lab_test_results import load as load_lab_test_results
from bronze.ingestion.load_bronze_supplier import load as load_supplier
from bronze.ingestion.load_bronze_samplers import load as load_samplers
from bronze.ingestion.load_bronze_mineral import load as load_mineral
from bronze.ingestion.load_bronze_locations import load as load_locations

def run_pipeline():
    logger = get_logger("bronze")
    logger.info("========================================")
    logger.info("Bronze pipeline started")
    logger.info("========================================")
    start_time = datetime.now()

    tasks = [
        ("bronze.sampling_requests", load_sampling_requests),
        ("bronze.task_assignment", load_task_assignment),
        ("bronze.sample_collection", load_sample_collection),
        ("bronze.lab_test_results", load_lab_test_results),
        ("bronze.supplier", load_supplier),
        ("bronze.samplers", load_samplers),
        ("bronze.mineral", load_mineral),
        ("bronze.locations", load_locations),
    ]

    for table_name, load_fn in tasks:
        success = load_fn()
        if not success:
            logger.error(f"Pipeline stopped at: {table_name}")
            logger.error("Bronze pipeline failed")
            sys.exit(1)

    duration = (datetime.now() - start_time).seconds
    logger.info("========================================")
    logger.info(f"Bronze pipeline completed | duration: {duration}s")
    logger.info("========================================")

if __name__ == "__main__":
    run_pipeline()