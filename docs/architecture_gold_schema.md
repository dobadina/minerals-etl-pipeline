# Gold Schema Design
## Minerals ETL Pipeline

---

## Overview
The Gold layer is modelled as a star schema optimised for Power BI reporting.
It consists of one fact table and five dimension tables. All tables are fully
joined and business-friendly — no hash IDs exposed to end users, all columns
renamed to plain English. Surrogate integer keys are used throughout to
replace the AppSheet hash IDs.

---

## Star Schema Diagram

                    dim_date
                       |
    dim_mineral   dim_region
           \       /
            fact_sampling_requests
           /       \
    dim_supplier   dim_sampler

---

## Grain
One row in fact_sampling_requests represents one sampling request.
This is the finest level of detail available in the source data.

---

## gold.dim_supplier
Source: silver.supplier

| Column Name | Data Type | Description |
|---|---|---|
| supplier_key | INTEGER | Surrogate key (PK) |
| supplier_id | VARCHAR | Original AppSheet ID |
| supplier_name | VARCHAR | Supplier full name |
| phone_number | VARCHAR | Standardised phone number |

---

## gold.dim_sampler
Source: silver.samplers

| Column Name | Data Type | Description |
|---|---|---|
| sampler_key | INTEGER | Surrogate key (PK) |
| sampler_id | VARCHAR | Original AppSheet ID |
| full_name | VARCHAR | Sampler full name |
| phone_number | VARCHAR | Standardised phone number |
| state_assigned | VARCHAR | State the sampler operates in |
| region_assigned | VARCHAR | Region the sampler operates in |
| email | VARCHAR | Sampler email address |

---

## gold.dim_mineral
Source: silver.mineral

| Column Name | Data Type | Description |
|---|---|---|
| mineral_key | INTEGER | Surrogate key (PK) |
| mineral_type_id | VARCHAR | Original AppSheet ID |
| mineral_name | VARCHAR | Mineral name |

---

## gold.dim_region
Source: silver.locations

| Column Name | Data Type | Description |
|---|---|---|
| region_key | INTEGER | Surrogate key (PK) |
| location_id | VARCHAR | Original AppSheet ID |
| state_name | VARCHAR | State name |
| region_name | VARCHAR | Region name |

---

## gold.dim_date
Source: Derived — generated from date range of sampling requests

| Column Name | Data Type | Description |
|---|---|---|
| date_key | INTEGER | Surrogate key in YYYYMMDD format (PK) |
| full_date | DATE | Full date |
| day | INTEGER | Day of month |
| month | INTEGER | Month number |
| month_name | VARCHAR | Month name e.g. January |
| quarter | INTEGER | Quarter number e.g. 1 |
| year | INTEGER | Year |
| day_of_week | INTEGER | Day of week number |
| day_name | VARCHAR | Day name e.g. Monday |
| is_weekend | BOOLEAN | True if Saturday or Sunday |

---

## gold.fact_sampling_requests
Source: silver.sampling_requests, silver.task_assignment, 
        silver.sample_collection, silver.lab_test_results

| Column Name | Data Type | Description |
|---|---|---|
| fact_key | INTEGER | Surrogate key (PK) |
| request_id | VARCHAR | Original AppSheet request ID |
| task_id | VARCHAR | Original AppSheet task ID |
| collection_id | VARCHAR | Original AppSheet collection ID |
| test_result_id | VARCHAR | Original AppSheet test result ID |
| supplier_key | INTEGER | FK to dim_supplier |
| sampler_key | INTEGER | FK to dim_sampler |
| mineral_key | INTEGER | FK to dim_mineral |
| region_key | INTEGER | FK to dim_region |
| request_date_key | INTEGER | FK to dim_date (request created date) |
| dispatch_date_key | INTEGER | FK to dim_date (dispatch date) |
| result_date_key | INTEGER | FK to dim_date (result received date) |
| scm_name | VARCHAR | Name of SCM who raised the request |
| request_status | VARCHAR | Current status of the request |
| task_status | VARCHAR | Current status of the task |
| sampler_acceptance_status | VARCHAR | Whether sampler accepted the task |
| collection_status | VARCHAR | Status of sample collection |
| dispatch_status | VARCHAR | Status of sample dispatch |
| analysis_status | VARCHAR | Lab analysis status |
| was_sample_received | VARCHAR | Whether lab received the sample |
| state | VARCHAR | State where sampling took place |
| region | VARCHAR | Region where sampling took place |
| request_to_dispatch_days | INTEGER | Days from request created to dispatch date |
| request_to_result_days | INTEGER | Days from request created to result received |
| ingestion_at | TIMESTAMP | Pipeline run timestamp |

---

## Measures for Power BI
These will be created as DAX measures in Power BI Desktop:

| Measure | Description |
|---|---|
| Total Requests | Count of all sampling requests |
| Completed Requests | Count of requests with collection status = Collected |
| Pending Requests | Count of requests with no task assigned |
| Avg Request to Dispatch Days | Average request_to_dispatch_days |
| Avg Request to Result Days | Average request_to_result_days |
| Sampler Acceptance Rate | % of tasks where sampler_acceptance_status = Accepted |
| Lab Result Rate | % of requests with analysis_status = Completed |

---

## Design Decisions
| Decision | Choice | Reason |
|---|---|---|
| Surrogate keys | INTEGER | Replaces hash IDs for Power BI relationship performance |
| Original IDs kept | VARCHAR | Preserves traceability back to source |
| Turnaround columns in fact | Derived integers | Enables simple DAX measures in Power BI |
| dim_date generated | YYYYMMDD key | Standard date dimension pattern for time intelligence |
| Fact joins 4 Silver tables | LEFT JOIN | Preserves all requests even without task or result |
| Photo/document columns excluded | Dropped | Not needed for analytics reporting |