import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection
from utils.logging_utils import get_logger

def validate():
    logger = get_logger("gold")
    logger.info("========================================")
    logger.info("Gold validation started")
    logger.info("========================================")

    conn = get_connection()
    all_passed = True

    # --- Fact row count matches sampling_requests ---
    logger.info("\n--- Fact Row Count Check ---")
    source_count = conn.execute(
        "SELECT COUNT(*) FROM silver.sampling_requests"
    ).fetchone()[0]
    fact_count = conn.execute(
        "SELECT COUNT(*) FROM gold.fact_sampling_requests"
    ).fetchone()[0]

    if source_count == fact_count:
        logger.info(f"PASS | fact_sampling_requests | expected: {source_count} | actual: {fact_count}")
    else:
        logger.error(f"FAIL | fact_sampling_requests | expected: {source_count} | actual: {fact_count}")
        all_passed = False

    # --- No duplicate request_ids in fact ---
    logger.info("\n--- Fact Duplicate Check ---")
    duplicate_count = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT request_id, COUNT(*) as cnt
            FROM gold.fact_sampling_requests
            GROUP BY request_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    if duplicate_count == 0:
        logger.info("PASS | fact_sampling_requests | no duplicate request_ids")
    else:
        logger.error(f"FAIL | fact_sampling_requests | {duplicate_count} duplicate request_ids found")
        all_passed = False

    # --- Dimension row counts ---
    logger.info("\n--- Dimension Row Count Checks ---")
    dim_checks = [
        ("gold.dim_supplier", "silver.supplier"),
        ("gold.dim_sampler", "silver.samplers"),
        ("gold.dim_region", "silver.locations"),
    ]

    for gold_table, silver_table in dim_checks:
        gold_count = conn.execute(f"SELECT COUNT(*) FROM {gold_table}").fetchone()[0]
        silver_count = conn.execute(f"SELECT COUNT(*) FROM {silver_table}").fetchone()[0]
        if gold_count == silver_count:
            logger.info(f"PASS | {gold_table} | rows: {gold_count}")
        else:
            logger.error(f"FAIL | {gold_table} | expected: {silver_count} | actual: {gold_count}")
            all_passed = False

    # --- Mineral dimension has 12 rows (Test removed) ---
    logger.info("\n--- Mineral Dimension Check ---")
    mineral_count = conn.execute(
        "SELECT COUNT(*) FROM gold.dim_mineral"
    ).fetchone()[0]
    if mineral_count == 12:
        logger.info(f"PASS | gold.dim_mineral | rows: {mineral_count}")
    else:
        logger.error(f"FAIL | gold.dim_mineral | expected: 12 | actual: {mineral_count}")
        all_passed = False

    # --- Surrogate keys are unique in all dimensions ---
    logger.info("\n--- Surrogate Key Uniqueness Checks ---")
    key_checks = [
        ("gold.dim_supplier", "supplier_key"),
        ("gold.dim_sampler", "sampler_key"),
        ("gold.dim_mineral", "mineral_key"),
        ("gold.dim_region", "region_key"),
        ("gold.dim_date", "date_key"),
    ]

    for table, key_col in key_checks:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        unique = conn.execute(
            f"SELECT COUNT(DISTINCT {key_col}) FROM {table}"
        ).fetchone()[0]
        if total == unique:
            logger.info(f"PASS | {table}.{key_col} | all unique")
        else:
            logger.error(f"FAIL | {table}.{key_col} | total: {total} | unique: {unique}")
            all_passed = False

    # --- Fact foreign keys resolve to dimensions ---
    logger.info("\n--- Foreign Key Resolution Checks ---")

    unresolved_supplier = conn.execute("""
        SELECT COUNT(*) FROM gold.fact_sampling_requests f
        LEFT JOIN gold.dim_supplier d ON f.supplier_key = d.supplier_key
        WHERE f.supplier_key IS NOT NULL AND d.supplier_key IS NULL
    """).fetchone()[0]
    if unresolved_supplier == 0:
        logger.info("PASS | fact.supplier_key | all keys resolve to dim_supplier")
    else:
        logger.error(f"FAIL | fact.supplier_key | {unresolved_supplier} unresolved keys")
        all_passed = False

    unresolved_mineral = conn.execute("""
        SELECT COUNT(*) FROM gold.fact_sampling_requests f
        LEFT JOIN gold.dim_mineral d ON f.mineral_key = d.mineral_key
        WHERE f.mineral_key IS NOT NULL AND d.mineral_key IS NULL
    """).fetchone()[0]
    if unresolved_mineral == 0:
        logger.info("PASS | fact.mineral_key | all keys resolve to dim_mineral")
    else:
        logger.error(f"FAIL | fact.mineral_key | {unresolved_mineral} unresolved keys")
        all_passed = False

    unresolved_region = conn.execute("""
        SELECT COUNT(*) FROM gold.fact_sampling_requests f
        LEFT JOIN gold.dim_region d ON f.region_key = d.region_key
        WHERE f.region_key IS NOT NULL AND d.region_key IS NULL
    """).fetchone()[0]
    if unresolved_region == 0:
        logger.info("PASS | fact.region_key | all keys resolve to dim_region")
    else:
        logger.error(f"FAIL | fact.region_key | {unresolved_region} unresolved keys")
        all_passed = False

    # --- Date keys resolve to dim_date ---
    logger.info("\n--- Date Key Resolution Checks ---")
    unresolved_request_date = conn.execute("""
        SELECT COUNT(*) FROM gold.fact_sampling_requests f
        LEFT JOIN gold.dim_date d ON f.request_date_key = d.date_key
        WHERE f.request_date_key IS NOT NULL AND d.date_key IS NULL
    """).fetchone()[0]
    if unresolved_request_date == 0:
        logger.info("PASS | fact.request_date_key | all keys resolve to dim_date")
    else:
        logger.error(f"FAIL | fact.request_date_key | {unresolved_request_date} unresolved keys")
        all_passed = False

    # --- Turnaround days sanity check ---
    logger.info("\n--- Turnaround Days Sanity Check ---")
    negative_days = conn.execute("""
        SELECT COUNT(*) FROM gold.fact_sampling_requests
        WHERE request_to_dispatch_days < 0
    """).fetchone()[0]
    if negative_days == 0:
        logger.info("PASS | request_to_dispatch_days | no negative values")
    else:
        logger.error(f"FAIL | request_to_dispatch_days | {negative_days} negative values found")
        all_passed = False

    # --- Summary stats ---
    logger.info("\n--- Summary Statistics ---")
    stats = conn.execute("""
        SELECT
            COUNT(*) as total_requests,
            COUNT(task_id) as assigned_tasks,
            COUNT(collection_id) as collections,
            COUNT(test_result_id) as lab_results,
            COUNT(request_to_dispatch_days) as dispatched,
            ROUND(AVG(request_to_dispatch_days), 1) as avg_dispatch_days
        FROM gold.fact_sampling_requests
    """).fetchone()

    logger.info(f"  Total requests:      {stats[0]}")
    logger.info(f"  Assigned tasks:      {stats[1]}")
    logger.info(f"  Collections:         {stats[2]}")
    logger.info(f"  Lab results:         {stats[3]}")
    logger.info(f"  Dispatched:          {stats[4]}")
    logger.info(f"  Avg dispatch days:   {stats[5]}")

    conn.close()
    logger.info("\n========================================")
    if all_passed:
        logger.info("Gold validation completed | all checks passed")
    else:
        logger.error("Gold validation completed | some checks failed")
    logger.info("========================================")

    return all_passed

if __name__ == "__main__":
    validate()