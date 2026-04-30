import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection
from utils.logging_utils import get_logger

def explore():
    logger = get_logger("silver")
    conn = get_connection()

    logger.info("========================================")
    logger.info("Bronze exploration started")
    logger.info("========================================")

    # --- sampling_requests ---
    logger.info("\n--- bronze.sampling_requests ---")

    logger.info("\nSample created_at values (date serial numbers):")
    rows = conn.execute("""
        SELECT created_at FROM bronze.sampling_requests LIMIT 5
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]}")

    logger.info("\nUnique request_status values:")
    rows = conn.execute("""
        SELECT request_status, COUNT(*) as cnt 
        FROM bronze.sampling_requests 
        GROUP BY request_status
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    logger.info("\nUnique next_step values:")
    rows = conn.execute("""
        SELECT next_step, COUNT(*) as cnt 
        FROM bronze.sampling_requests 
        GROUP BY next_step
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    logger.info("\nSample comments with #ERROR!:")
    rows = conn.execute("""
        SELECT request_id, comments 
        FROM bronze.sampling_requests 
        WHERE comments LIKE '%ERROR%' 
        LIMIT 5
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} | {r[1]}")

    logger.info("\nSample supplier_phone values:")
    rows = conn.execute("""
        SELECT DISTINCT supplier_phone 
        FROM bronze.sampling_requests 
        LIMIT 10
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]}")

    # --- task_assignment ---
    logger.info("\n--- bronze.task_assignment ---")

    logger.info("\nUnique task_status values:")
    rows = conn.execute("""
        SELECT task_status, COUNT(*) as cnt 
        FROM bronze.task_assignment 
        GROUP BY task_status
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    logger.info("\nUnique sampler_acceptance_status values:")
    rows = conn.execute("""
        SELECT sampler_acceptance_status, COUNT(*) as cnt 
        FROM bronze.task_assignment 
        GROUP BY sampler_acceptance_status
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    logger.info("\nNull breakdown for key columns:")
    rows = conn.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(assigned_sampler_id) as has_sampler,
            COUNT(lab_id) as has_lab,
            COUNT(assigned_at) as has_assigned_at
        FROM bronze.task_assignment
    """).fetchall()
    for r in rows: logger.info(f"  total: {r[0]} | has_sampler: {r[1]} | has_lab: {r[2]} | has_assigned_at: {r[3]}")

    # --- sample_collection ---
    logger.info("\n--- bronze.sample_collection ---")

    logger.info("\nUnique collection_status values:")
    rows = conn.execute("""
        SELECT collection_status, COUNT(*) as cnt 
        FROM bronze.sample_collection 
        GROUP BY collection_status
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    logger.info("\nUnique dispatch_status values:")
    rows = conn.execute("""
        SELECT dispatch_status, COUNT(*) as cnt 
        FROM bronze.sample_collection 
        GROUP BY dispatch_status
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    logger.info("\nUnique collection_acceptance_status values:")
    rows = conn.execute("""
        SELECT collection_acceptance_status, COUNT(*) as cnt 
        FROM bronze.sample_collection 
        GROUP BY collection_acceptance_status
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    logger.info("\nSupplier phone errors:")
    rows = conn.execute("""
        SELECT COUNT(*) as error_count
        FROM bronze.sample_collection
        WHERE supplier_phone LIKE '%ERROR%'
    """).fetchall()
    for r in rows: logger.info(f"  error count: {r[0]}")

    logger.info("\nNull breakdown for key columns:")
    rows = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(collection_date) as has_collection_date,
            COUNT(dispatch_date) as has_dispatch_date,
            COUNT(driver_id) as has_driver
        FROM bronze.sample_collection
    """).fetchall()
    for r in rows: logger.info(f"  total: {r[0]} | has_collection_date: {r[1]} | has_dispatch_date: {r[2]} | has_driver: {r[3]}")

    # --- lab_test_results ---
    logger.info("\n--- bronze.lab_test_results ---")

    logger.info("\nNull breakdown for key columns:")
    rows = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(result_received_date) as has_result_date,
            COUNT(analysis_status) as has_analysis_status,
            COUNT(was_sample_received) as has_received_flag
        FROM bronze.lab_test_results
    """).fetchall()
    for r in rows: logger.info(f"  total: {r[0]} | has_result_date: {r[1]} | has_analysis_status: {r[2]} | has_received_flag: {r[3]}")

    logger.info("\nUnique analysis_status values:")
    rows = conn.execute("""
        SELECT analysis_status, COUNT(*) as cnt 
        FROM bronze.lab_test_results 
        GROUP BY analysis_status
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    logger.info("\nUnique was_sample_received values:")
    rows = conn.execute("""
        SELECT was_sample_received, COUNT(*) as cnt 
        FROM bronze.lab_test_results 
        GROUP BY was_sample_received
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    # --- supplier ---
    logger.info("\n--- bronze.supplier ---")

    logger.info("\nSample supplier names (checking for duplicates):")
    rows = conn.execute("""
        SELECT supplier_name, COUNT(*) as cnt 
        FROM bronze.supplier 
        GROUP BY supplier_name 
        HAVING COUNT(*) > 1
    """).fetchall()
    if rows:
        for r in rows: logger.info(f"  {r[0]} — {r[1]} records")
    else:
        logger.info("  No duplicate supplier names found")

    logger.info("\nSample phone numbers:")
    rows = conn.execute("""
        SELECT DISTINCT phone_number 
        FROM bronze.supplier 
        LIMIT 10
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]}")

    # --- samplers ---
    logger.info("\n--- bronze.samplers ---")

    logger.info("\nNull breakdown:")
    rows = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(region_assigned) as has_region
        FROM bronze.samplers
    """).fetchall()
    for r in rows: logger.info(f"  total: {r[0]} | has_region: {r[1]}")

    logger.info("\nUnique regions:")
    rows = conn.execute("""
        SELECT region_assigned, COUNT(*) as cnt 
        FROM bronze.samplers 
        GROUP BY region_assigned
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    # --- mineral ---
    logger.info("\n--- bronze.mineral ---")

    logger.info("\nAll mineral names:")
    rows = conn.execute("""
        SELECT mineral_name FROM bronze.mineral ORDER BY mineral_name
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]}")

    # --- locations ---
    logger.info("\n--- bronze.locations ---")

    logger.info("\nAll regions:")
    rows = conn.execute("""
        SELECT region_name, COUNT(*) as cnt 
        FROM bronze.locations 
        GROUP BY region_name 
        ORDER BY region_name
    """).fetchall()
    for r in rows: logger.info(f"  {r[0]} — {r[1]}")

    conn.close()
    logger.info("\n========================================")
    logger.info("Bronze exploration complete")
    logger.info("========================================")

if __name__ == "__main__":
    explore()