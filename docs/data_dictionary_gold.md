# Data Dictionary — Gold Layer
## Minerals ETL Pipeline

---

## gold.fact_sampling_requests
**Description:** One row per sampling request. Central fact table linking all 
dimensions and capturing the full lifecycle of a sampling request from creation 
through to lab result.
**Grain:** One sampling request

| Column | Data Type | Description | Example |
|---|---|---|---|
| fact_key | INTEGER | Surrogate primary key | 1 |
| request_id | VARCHAR | Original AppSheet request ID | f1fd1f31 |
| task_id | VARCHAR | AppSheet task ID (NULL if unassigned) | a1b2c3d4 |
| collection_id | VARCHAR | AppSheet collection ID (NULL if not collected) | e5f6g7h8 |
| test_result_id | VARCHAR | AppSheet lab result ID (NULL if no result) | i9j0k1l2 |
| supplier_key | INTEGER | FK to dim_supplier | 42 |
| sampler_key | INTEGER | FK to dim_sampler (NULL if unassigned) | 7 |
| mineral_key | INTEGER | FK to dim_mineral | 3 |
| region_key | INTEGER | FK to dim_region | 15 |
| request_date_key | INTEGER | FK to dim_date — date request was created | 20250304 |
| dispatch_date_key | INTEGER | FK to dim_date — date sample was dispatched (NULL if not dispatched) | 20250310 |
| result_date_key | INTEGER | FK to dim_date — date lab result received (NULL if no result) | 20250320 |
| scm_name | VARCHAR | Name of Supply Chain Manager who raised the request | Oluwaseyi |
| request_status | VARCHAR | Status of the request | Requested |
| task_status | VARCHAR | Status of the task assignment (NULL if unassigned) | Assigned |
| sampler_acceptance_status | VARCHAR | Whether sampler accepted or declined (NULL if not responded) | Declined |
| collection_status | VARCHAR | Status of sample collection | Sample Picked |
| dispatch_status | VARCHAR | Status of sample dispatch | Sample Dispatched |
| analysis_status | VARCHAR | Lab analysis status (NULL if not processed) | Analysis Completed |
| was_sample_received | VARCHAR | Whether lab confirmed receipt (NULL if not processed) | Sample Received |
| state | VARCHAR | Nigerian state where sampling took place | Nasarawa |
| region | VARCHAR | Geopolitical region where sampling took place | North Central |
| request_to_dispatch_days | INTEGER | Days from request creation to sample dispatch (NULL if not dispatched) | 6 |
| request_to_result_days | INTEGER | Days from request creation to lab result (NULL if no result) | 16 |
| ingestion_at | TIMESTAMP | Timestamp when Gold pipeline ran | 2026-04-30 20:53:49 |

---

## gold.dim_supplier
**Description:** One row per supplier. Contains all active mineral suppliers 
across the 12-state sourcing operation.
**Grain:** One supplier

| Column | Data Type | Description | Example |
|---|---|---|---|
| supplier_key | INTEGER | Surrogate primary key | 42 |
| supplier_id | VARCHAR | Original AppSheet supplier ID | 59bda3eb |
| supplier_name | VARCHAR | Supplier full name (title case) | Ibrahim |
| phone_number | VARCHAR | Standardised phone number in +234XXXXXXXXXX format | +2348023138652 |

---

## gold.dim_sampler
**Description:** One row per field sampler. Contains all active samplers 
assigned to sampling tasks across the operation.
**Grain:** One sampler

| Column | Data Type | Description | Example |
|---|---|---|---|
| sampler_key | INTEGER | Surrogate primary key | 7 |
| sampler_id | VARCHAR | Original AppSheet sampler ID | a1b2c3d4 |
| full_name | VARCHAR | Sampler full name (title case) | Kitchiwe Andrew |
| phone_number | VARCHAR | Standardised phone number in +234XXXXXXXXXX format | +2348012345678 |
| state_assigned | VARCHAR | Nigerian state the sampler is based in | Plateau |
| email | VARCHAR | Sampler email address (lowercase) | andrew@email.com |

---

## gold.dim_mineral
**Description:** One row per mineral type. Test record excluded.
**Grain:** One mineral type

| Column | Data Type | Description | Example |
|---|---|---|---|
| mineral_key | INTEGER | Surrogate primary key | 3 |
| mineral_type_id | VARCHAR | Original AppSheet mineral ID | 399147c9 |
| mineral_name | VARCHAR | Mineral name (title case) | Spodumene |

**Valid mineral names:** Antimony, Beryllium, Columbite, Copper, Ilmenite, 
Lead, Monazite, Spodumene, Tantalite, Tin, Wolframite, Zinc

---

## gold.dim_region
**Description:** One row per Nigerian state with its geopolitical region.
**Grain:** One state

| Column | Data Type | Description | Example |
|---|---|---|---|
| region_key | INTEGER | Surrogate primary key | 15 |
| location_id | VARCHAR | Original AppSheet location ID | 806612b7 |
| state_name | VARCHAR | Nigerian state name (title case) | Nasarawa |
| region_name | VARCHAR | Nigerian geopolitical region (title case) | North Central |

**Valid regions:** North Central, North East, North West, South East, 
South South, South West

---

## gold.dim_date
**Description:** Standard date dimension covering the full range of sampling 
request dates. Generated from the minimum to maximum created_at date in 
silver.sampling_requests.
**Grain:** One calendar day

| Column | Data Type | Description | Example |
|---|---|---|---|
| date_key | INTEGER | Surrogate primary key in YYYYMMDD format | 20250304 |
| full_date | DATE | Full calendar date | 2025-03-04 |
| day | INTEGER | Day of month | 4 |
| month | INTEGER | Month number | 3 |
| month_name | VARCHAR | Full month name | March |
| quarter | INTEGER | Quarter number | 1 |
| year | INTEGER | Calendar year | 2025 |
| day_of_week | INTEGER | Day of week (1=Monday, 7=Sunday) | 2 |
| day_name | VARCHAR | Full day name | Tuesday |
| is_weekend | BOOLEAN | True if Saturday or Sunday | false |

---

## Status Value Reference

### request_status
| Value | Meaning |
|---|---|
| Requested | Request created, awaiting sampler assignment |

### task_status
| Value | Meaning |
|---|---|
| Requested | Task created but sampler not yet assigned |
| Assigned | Sampler has been assigned to the task |

### sampler_acceptance_status
| Value | Meaning |
|---|---|
| NULL | Sampler has not responded (MNAR) |
| Declined | Sampler declined the task |

### collection_status
| Value | Meaning |
|---|---|
| Assigned | Sampler assigned but sample not yet collected |
| Sample Picked | Sample has been collected from the supplier |

### dispatch_status
| Value | Meaning |
|---|---|
| Sample Picked | Sample collected but not yet dispatched to lab |
| Sample Dispatched | Sample has been sent to the lab |

### analysis_status
| Value | Meaning |
|---|---|
| NULL | Lab has not processed the sample yet (MNAR) |
| Analysis Completed | Lab has completed analysis |

### was_sample_received
| Value | Meaning |
|---|---|
| NULL | Lab has not confirmed receipt (MNAR) |
| Sample Received | Lab has confirmed receipt of sample |