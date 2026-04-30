import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection
from utils.logging_utils import get_logger

def validate():
    logger = get_logger("silver")
    logger.info("========================================")
    logger.info("Silver validation started")
    logger.info("========================================")

    conn = get_connection()
    all_passed = True

    # --- Row count checks vs Bronze ---
    logger.info("\n--- Row Count Checks ---")
    row_checks = [
        ("bronze.sampling_requests", "silver.sampling_requests"),
        ("bronze.task_assignment", "silver.task_assignment"),
        ("bronze.sample_collection", "silver.sample_collection"),
        ("bronze.lab_test_results", "silver.lab_test_results"),
        ("bronze.supplier", "silver.supplier"),
        ("bronze.samplers", "silver.samplers"),
        ("bronze.locations", "silver.locations"),
    ]

    for bronze_table, silver_table in row_checks:
        bronze_count = conn.execute(
            f"SELECT COUNT(*) FROM {bronze_table}"
        ).fetchone()[0]
        silver_count = conn.execute(
            f"SELECT COUNT(*) FROM {silver_table}"
        ).fetchone()[0]

        if bronze_count == silver_count:
            logger.info(f"PASS | {silver_table} | bronze: {bronze_count} | silver: {silver_count}")
        else:
            logger.error(f"FAIL | {silver_table} | bronze: {bronze_count} | silver: {silver_count}")
            all_passed = False

    # --- Mineral test record removed ---
    logger.info("\n--- Mineral Test Record Check ---")
    test_count = conn.execute("""
        SELECT COUNT(*) FROM silver.mineral WHERE mineral_name = 'Test'
    """).fetchone()[0]
    if test_count == 0:
        logger.info("PASS | silver.mineral | Test record correctly removed")
    else:
        logger.error("FAIL | silver.mineral | Test record still present")
        all_passed = False

    # --- No #ERROR! values in Silver ---
    logger.info("\n--- Error Value Checks ---")
    error_checks = [
        ("silver.sampling_requests", "comments"),
        ("silver.sample_collection", "supplier_phone"),
    ]
    for table, column in error_checks:
        error_count = conn.execute(f"""
            SELECT COUNT(*) FROM {table}
            WHERE {column} LIKE '%ERROR%'
        """).fetchone()[0]
        if error_count == 0:
            logger.info(f"PASS | {table}.{column} | no #ERROR! values")
        else:
            logger.error(f"FAIL | {table}.{column} | {error_count} #ERROR! values remain")
            all_passed = False

    # --- collection_acceptance_status has no datetime values ---
    logger.info("\n--- collection_acceptance_status Check ---")
    bad_status_count = conn.execute("""
        SELECT COUNT(*) FROM silver.sample_collection
        WHERE collection_acceptance_status NOT IN ('Accepted', 'Declined')
        AND collection_acceptance_status IS NOT NULL
    """).fetchone()[0]
    if bad_status_count == 0:
        logger.info("PASS | silver.sample_collection.collection_acceptance_status | no invalid values")
    else:
        logger.error(f"FAIL | silver.sample_collection.collection_acceptance_status | {bad_status_count} invalid values")
        all_passed = False

    # --- region_assigned all null in samplers ---
    logger.info("\n--- Samplers region_assigned Check ---")
    non_null_region = conn.execute("""
        SELECT COUNT(*) FROM silver.samplers
        WHERE region_assigned IS NOT NULL
    """).fetchone()[0]
    if non_null_region == 0:
        logger.info("PASS | silver.samplers.region_assigned | all null as expected")
    else:
        logger.error(f"FAIL | silver.samplers.region_assigned | {non_null_region} non-null values remain")
        all_passed = False

    # --- Date columns are not null where expected ---
    logger.info("\n--- Date Column Checks ---")
    date_checks = [
        ("silver.sampling_requests", "created_at", 0),
        ("silver.task_assignment", "request_created_date", 0),
    ]
    for table, column, expected_nulls in date_checks:
        null_count = conn.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE {column} IS NULL
        """).fetchone()[0]
        if null_count == expected_nulls:
            logger.info(f"PASS | {table}.{column} | null count: {null_count}")
        else:
            logger.error(f"FAIL | {table}.{column} | null count: {null_count} (expected {expected_nulls})")
            all_passed = False

    # --- ingestion_at populated on all tables ---
    logger.info("\n--- ingestion_at Checks ---")
    silver_tables = [
        "silver.sampling_requests",
        "silver.task_assignment",
        "silver.sample_collection",
        "silver.lab_test_results",
        "silver.supplier",
        "silver.samplers",
        "silver.mineral",
        "silver.locations",
    ]
    for table in silver_tables:
        null_count = conn.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE ingestion_at IS NULL
        """).fetchone()[0]
        if null_count == 0:
            logger.info(f"PASS | {table}.ingestion_at | all populated")
        else:
            logger.error(f"FAIL | {table}.ingestion_at | {null_count} nulls found")
            all_passed = False

    conn.close()
    logger.info("\n========================================")
    if all_passed:
        logger.info("Silver validation completed | all checks passed")
    else:
        logger.error("Silver validation completed | some checks failed")
    logger.info("========================================")

    return all_passed

if __name__ == "__main__":
    validate()