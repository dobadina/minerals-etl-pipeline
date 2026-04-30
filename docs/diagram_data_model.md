# Data Model Diagram — Gold Layer
## Minerals ETL Pipeline

---

````mermaid
erDiagram
    fact_sampling_requests {
        INTEGER fact_key PK
        VARCHAR request_id
        VARCHAR task_id
        VARCHAR collection_id
        VARCHAR test_result_id
        INTEGER supplier_key FK
        INTEGER sampler_key FK
        INTEGER mineral_key FK
        INTEGER region_key FK
        INTEGER request_date_key FK
        INTEGER dispatch_date_key FK
        INTEGER result_date_key FK
        VARCHAR scm_name
        VARCHAR request_status
        VARCHAR task_status
        VARCHAR sampler_acceptance_status
        VARCHAR collection_status
        VARCHAR dispatch_status
        VARCHAR analysis_status
        VARCHAR was_sample_received
        VARCHAR state
        VARCHAR region
        INTEGER request_to_dispatch_days
        INTEGER request_to_result_days
        TIMESTAMP ingestion_at
    }

    dim_supplier {
        INTEGER supplier_key PK
        VARCHAR supplier_id
        VARCHAR supplier_name
        VARCHAR phone_number
    }

    dim_sampler {
        INTEGER sampler_key PK
        VARCHAR sampler_id
        VARCHAR full_name
        VARCHAR phone_number
        VARCHAR state_assigned
        VARCHAR email
    }

    dim_mineral {
        INTEGER mineral_key PK
        VARCHAR mineral_type_id
        VARCHAR mineral_name
    }

    dim_region {
        INTEGER region_key PK
        VARCHAR location_id
        VARCHAR state_name
        VARCHAR region_name
    }

    dim_date {
        INTEGER date_key PK
        DATE full_date
        INTEGER day
        INTEGER month
        VARCHAR month_name
        INTEGER quarter
        INTEGER year
        INTEGER day_of_week
        VARCHAR day_name
        BOOLEAN is_weekend
    }

    fact_sampling_requests }o--|| dim_supplier : "supplier_key"
    fact_sampling_requests }o--|| dim_sampler : "sampler_key"
    fact_sampling_requests }o--|| dim_mineral : "mineral_key"
    fact_sampling_requests }o--|| dim_region : "region_key"
    fact_sampling_requests }o--|| dim_date : "request_date_key"
    fact_sampling_requests }o--|| dim_date : "dispatch_date_key"
    fact_sampling_requests }o--|| dim_date : "result_date_key"
````

---

## Relationship Notes

| Relationship | Type | Notes |
|---|---|---|
| fact → dim_supplier | Many to one | Every request has one supplier |
| fact → dim_sampler | Many to one | NULL when task not yet assigned |
| fact → dim_mineral | Many to one | Every request has one mineral type |
| fact → dim_region | Many to one | Based on state of sampling location |
| fact → dim_date (request) | Many to one | Always populated |
| fact → dim_date (dispatch) | Many to one | NULL when not yet dispatched |
| fact → dim_date (result) | Many to one | NULL when no lab result yet |

---

## Key Metrics Derivable from This Model

| Metric | Calculation |
|---|---|
| Total Requests | COUNT(fact_key) |
| Assigned Tasks | COUNT(task_id) |
| Collected Samples | COUNT(collection_id) |
| Dispatched Samples | COUNT(dispatch_date_key) |
| Lab Results Received | COUNT(test_result_id) |
| Avg Request to Dispatch | AVG(request_to_dispatch_days) |
| Avg Request to Result | AVG(request_to_result_days) |
| Sampler Acceptance Rate | COUNT(task_id where sampler_acceptance_status != Declined) / COUNT(task_id) |
| Collection Rate | COUNT(collection_id) / COUNT(task_id) |
| Lab Result Rate | COUNT(test_result_id) / COUNT(collection_id) |
````
````