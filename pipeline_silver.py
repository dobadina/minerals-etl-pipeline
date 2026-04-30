import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logging_utils import get_logger
from silver.transformations.load_silver_sampling_requests import load as load_sampling_requests
from silver.transformations.load_silver_task_assignment import load as load_task_assignment
from silver.transformations.load_silver_sample_collection import load as load_sample_collection
from silver.transformations.load_silver_lab_test_results import load as load_lab_test_results
from silver.transformations.load_silver_supplier import load as load_supplier
from silver.transformations.load_silver_samplers import load as load_samplers
from silver.transformations.load_silver_mineral import load as load_mineral
from silver.transformations.load_silver_locations import load as load_locations

def run_pipeline():
    logger = get_logger("silver")
    logger.info("========================================")
    logger.info("Silver pipeline started")
    logger.info("========================================")
    start_time = datetime.now()

    tasks = [
        ("silver.sampling_requests", load_sampling_requests),
        ("silver.task_assignment", load_task_assignment),
        ("silver.sample_collection", load_sample_collection),
        ("silver.lab_test_results", load_lab_test_results),
        ("silver.supplier", load_supplier),
        ("silver.samplers", load_samplers),
        ("silver.mineral", load_mineral),
        ("silver.locations", load_locations),
    ]

    for table_name, load_fn in tasks:
        success = load_fn()
        if not success:
            logger.error(f"Pipeline stopped at: {table_name}")
            logger.error("Silver pipeline failed")
            sys.exit(1)

    duration = (datetime.now() - start_time).seconds
    logger.info("========================================")
    logger.info(f"Silver pipeline completed | duration: {duration}s")
    logger.info("========================================")

if __name__ == "__main__":
    run_pipeline()