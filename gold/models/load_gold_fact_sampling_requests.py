from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db_utils import get_connection, create_schema
from utils.logging_utils import get_logger

TABLE_NAME = "gold.fact_sampling_requests"

def load():
    logger = get_logger("gold")
    logger.info(f"Starting load: {TABLE_NAME}")
    start_time = datetime.now()

    try:
        conn = get_connection()
        create_schema(conn, "gold")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                fact_key INTEGER,
                request_id VARCHAR,
                task_id VARCHAR,
                collection_id VARCHAR,
                test_result_id VARCHAR,
                supplier_key INTEGER,
                sampler_key INTEGER,
                mineral_key INTEGER,
                region_key INTEGER,
                request_date_key INTEGER,
                dispatch_date_key INTEGER,
                result_date_key INTEGER,
                scm_name VARCHAR,
                request_status VARCHAR,
                task_status VARCHAR,
                sampler_acceptance_status VARCHAR,
                collection_status VARCHAR,
                dispatch_status VARCHAR,
                analysis_status VARCHAR,
                was_sample_received VARCHAR,
                state VARCHAR,
                region VARCHAR,
                request_to_dispatch_days INTEGER,
                request_to_result_days INTEGER,
                ingestion_at TIMESTAMP
            )
        """)

        conn.execute(f"TRUNCATE {TABLE_NAME}")

        conn.execute(f"""
            INSERT INTO {TABLE_NAME}
            WITH latest_task AS (
                SELECT *
                FROM silver.task_assignment
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY request_id ORDER BY assigned_at DESC NULLS LAST
                ) = 1
            ),
            latest_collection AS (
                SELECT *
                FROM silver.sample_collection
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY request_id ORDER BY collection_date DESC NULLS LAST
                ) = 1
            ),
            latest_result AS (
                SELECT *
                FROM silver.lab_test_results
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY request_id ORDER BY result_received_date DESC NULLS LAST
                ) = 1
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY sr.request_id) AS fact_key,
                sr.request_id,
                ta.task_id,
                sc.collection_id,
                ltr.test_result_id,

                ds.supplier_key,
                dsam.sampler_key,
                dm.mineral_key,
                dr.region_key,

                CAST(STRFTIME(sr.created_at, '%Y%m%d') AS INTEGER)
                    AS request_date_key,
                CASE WHEN sc.dispatch_date IS NOT NULL
                    THEN CAST(STRFTIME(sc.dispatch_date, '%Y%m%d') AS INTEGER)
                    ELSE NULL END AS dispatch_date_key,
                CASE WHEN ltr.result_received_date IS NOT NULL
                    THEN CAST(STRFTIME(ltr.result_received_date, '%Y%m%d') AS INTEGER)
                    ELSE NULL END AS result_date_key,

                sr.scm_name,
                sr.request_status,
                ta.task_status,
                ta.sampler_acceptance_status,
                sc.collection_status,
                sc.dispatch_status,
                ltr.analysis_status,
                ltr.was_sample_received,
                sr.state,
                sr.region,

                CASE WHEN sc.dispatch_date IS NOT NULL
                    THEN DATEDIFF('day', sr.created_at, sc.dispatch_date)
                    ELSE NULL END AS request_to_dispatch_days,
                CASE WHEN ltr.result_received_date IS NOT NULL
                    THEN DATEDIFF('day', sr.created_at, ltr.result_received_date)
                    ELSE NULL END AS request_to_result_days,

                CURRENT_TIMESTAMP AS ingestion_at

            FROM silver.sampling_requests sr

            LEFT JOIN latest_task ta
                ON sr.request_id = ta.request_id

            LEFT JOIN latest_collection sc
                ON sr.request_id = sc.request_id

            LEFT JOIN latest_result ltr
                ON sr.request_id = ltr.request_id

            LEFT JOIN gold.dim_supplier ds
                ON sr.supplier_id = ds.supplier_id

            LEFT JOIN gold.dim_sampler dsam
                ON ta.assigned_sampler_id = dsam.sampler_id

            LEFT JOIN gold.dim_mineral dm
                ON sr.mineral_type_id = dm.mineral_type_id

            LEFT JOIN gold.dim_region dr
                ON sr.state = dr.state_name
        """)

        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {TABLE_NAME}"
        ).fetchone()[0]
        duration = (datetime.now() - start_time).seconds

        logger.info(
            f"Completed: {TABLE_NAME} | rows: {row_count} | duration: {duration}s"
        )
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Failed: {TABLE_NAME} | error: {str(e)}")
        return False

if __name__ == "__main__":
    load()