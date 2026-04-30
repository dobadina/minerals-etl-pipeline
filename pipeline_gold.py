import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logging_utils import get_logger
from gold.models.load_gold_dim_supplier import load as load_dim_supplier
from gold.models.load_gold_dim_sampler import load as load_dim_sampler
from gold.models.load_gold_dim_mineral import load as load_dim_mineral
from gold.models.load_gold_dim_region import load as load_dim_region
from gold.models.load_gold_dim_date import load as load_dim_date
from gold.models.load_gold_fact_sampling_requests import load as load_fact

def run_pipeline():
    logger = get_logger("gold")
    logger.info("========================================")
    logger.info("Gold pipeline started")
    logger.info("========================================")
    start_time = datetime.now()

    tasks = [
        ("gold.dim_supplier", load_dim_supplier),
        ("gold.dim_sampler", load_dim_sampler),
        ("gold.dim_mineral", load_dim_mineral),
        ("gold.dim_region", load_dim_region),
        ("gold.dim_date", load_dim_date),
        ("gold.fact_sampling_requests", load_fact),
    ]

    for table_name, load_fn in tasks:
        success = load_fn()
        if not success:
            logger.error(f"Pipeline stopped at: {table_name}")
            logger.error("Gold pipeline failed")
            sys.exit(1)

    duration = (datetime.now() - start_time).seconds
    logger.info("========================================")
    logger.info(f"Gold pipeline completed | duration: {duration}s")
    logger.info("========================================")

if __name__ == "__main__":
    run_pipeline()