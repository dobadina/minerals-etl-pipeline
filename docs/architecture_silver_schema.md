# Silver Schema Design
## Minerals ETL Pipeline

---

## Overview
Silver tables store cleaned, standardised and enriched data from Bronze.
Proper data types are applied, dates are converted from Excel serial numbers,
phone numbers are standardised, #ERROR! values are resolved, and analytical
columns are derived. All ID columns remain VARCHAR to preserve the original
AppSheet hash IDs.

---

## silver.sampling_requests
Source: bronze.sampling_requests

| Column Name | Data Type | Transformation |
|---|---|---|
| request_id | VARCHAR | As-is from Bronze |
| scm_email | VARCHAR | Trimmed, lowercased |
| scm_name | VARCHAR | Trimmed |
| mineral_type_id | VARCHAR | As-is from Bronze |
| test_type_id | VARCHAR | As-is from Bronze |
| supplier_id | VARCHAR | As-is from Bronze |
| supplier_phone | VARCHAR | Standardised to +234XXXXXXXXXX format |
| pickup_location_id | VARCHAR | As-is from Bronze |
| state | VARCHAR | Trimmed |
| region | VARCHAR | Trimmed |
| created_at | DATE | Converted from Excel serial number |
| request_status | VARCHAR | Trimmed |
| status_updated_at | DATE | Converted from Excel serial number |
| comments | VARCHAR | #ERROR! values replaced with NULL |
| next_step | VARCHAR | Trimmed |
| ingestion_at | TIMESTAMP | Pipeline run timestamp |

---

## silver.task_assignment
Source: bronze.task_assignment

| Column Name | Data Type | Transformation |
|---|---|---|
| task_id | VARCHAR | As-is from Bronze |
| request_id | VARCHAR | As-is from Bronze |
| mineral_type_id | VARCHAR | As-is from Bronze |
| supplier_id | VARCHAR | As-is from Bronze |
| supplier_phone | VARCHAR | Standardised to +234XXXXXXXXXX format |
| pickup_location | VARCHAR | Trimmed |
| state | VARCHAR | Trimmed |
| region | VARCHAR | Trimmed |
| assigned_sampler_id | VARCHAR | As-is from Bronze |
| lab_id | VARCHAR | As-is from Bronze |
| task_status | VARCHAR | Trimmed |
| sampler_acceptance_status | VARCHAR | Trimmed |
| status_updated_at | DATE | Converted from Excel serial number |
| assigned_by | VARCHAR | As-is from Bronze |
| assigned_at | DATE | Converted from Excel serial number |
| request_created_date | DATE | Converted from Excel serial number |
| ingestion_at | TIMESTAMP | Pipeline run timestamp |

---

## silver.sample_collection
Source: bronze.sample_collection

| Column Name | Data Type | Transformation |
|---|---|---|
| collection_id | VARCHAR | As-is from Bronze |
| task_id | VARCHAR | As-is from Bronze |
| request_id | VARCHAR | As-is from Bronze |
| sampler_id | VARCHAR | As-is from Bronze |
| sampler_email | VARCHAR | Trimmed, lowercased |
| supplier_id | VARCHAR | As-is from Bronze |
| supplier_phone | VARCHAR | Standardised to +234XXXXXXXXXX format |
| pickup_location_id | VARCHAR | As-is from Bronze |
| collection_acceptance_status | VARCHAR | Trimmed |
| collection_acceptance_datetime | TIMESTAMP | Converted from Excel serial number |
| collection_date | DATE | Converted from Excel serial number |
| collection_status | VARCHAR | Trimmed |
| collection_status_updated_at | DATE | Converted from Excel serial number |
| driver_id | VARCHAR | As-is from Bronze |
| dispatch_date | DATE | Converted from Excel serial number |
| dispatch_status | VARCHAR | Trimmed |
| dispatch_status_updated_at | DATE | Converted from Excel serial number |
| bot_record_id | VARCHAR | As-is from Bronze |
| ingestion_at | TIMESTAMP | Pipeline run timestamp |

---

## silver.lab_test_results
Source: bronze.lab_test_results

| Column Name | Data Type | Transformation |
|---|---|---|
| test_result_id | VARCHAR | As-is from Bronze |
| collection_id | VARCHAR | As-is from Bronze |
| task_id | VARCHAR | As-is from Bronze |
| request_id | VARCHAR | As-is from Bronze |
| sampler_id | VARCHAR | As-is from Bronze |
| driver_id | VARCHAR | As-is from Bronze |
| result_document_url | VARCHAR | As-is from Bronze |
| uploaded_by | VARCHAR | Trimmed, lowercased |
| uploader_name | VARCHAR | Trimmed |
| result_received_date | DATE | Converted from Excel serial number |
| analysis_status | VARCHAR | Trimmed |
| status_updated_at | DATE | Converted from Excel serial number |
| was_sample_received | VARCHAR | Trimmed |
| ingestion_at | TIMESTAMP | Pipeline run timestamp |

---

## silver.supplier
Source: bronze.supplier

| Column Name | Data Type | Transformation |
|---|---|---|
| supplier_id | VARCHAR | As-is from Bronze |
| supplier_name | VARCHAR | Trimmed, title case |
| phone_number | VARCHAR | Standardised to +234XXXXXXXXXX format |
| ingestion_at | TIMESTAMP | Pipeline run timestamp |

---

## silver.samplers
Source: bronze.samplers

| Column Name | Data Type | Transformation |
|---|---|---|
| sampler_id | VARCHAR | As-is from Bronze |
| full_name | VARCHAR | Trimmed, title case |
| phone_number | VARCHAR | Standardised to +234XXXXXXXXXX format |
| state_assigned | VARCHAR | Trimmed |
| region_assigned | VARCHAR | Trimmed |
| email | VARCHAR | Trimmed, lowercased |
| ingestion_at | TIMESTAMP | Pipeline run timestamp |

---

## silver.mineral
Source: bronze.mineral

| Column Name | Data Type | Transformation |
|---|---|---|
| mineral_type_id | VARCHAR | As-is from Bronze |
| mineral_name | VARCHAR | Trimmed, title case |
| ingestion_at | TIMESTAMP | Pipeline run timestamp |

---

## silver.locations
Source: bronze.locations

| Column Name | Data Type | Transformation |
|---|---|---|
| location_id | VARCHAR | As-is from Bronze |
| state_name | VARCHAR | Trimmed, title case |
| region_name | VARCHAR | Trimmed, title case |
| ingestion_at | TIMESTAMP | Pipeline run timestamp |

---

## Derived Columns
These columns do not exist in Bronze — they are calculated during 
Silver transformation:

| Table | Column | Logic |
|---|---|---|
| silver.sampling_requests | ingestion_at | CURRENT_TIMESTAMP at pipeline run time |
| silver.task_assignment | ingestion_at | CURRENT_TIMESTAMP at pipeline run time |
| silver.sample_collection | ingestion_at | CURRENT_TIMESTAMP at pipeline run time |
| silver.lab_test_results | ingestion_at | CURRENT_TIMESTAMP at pipeline run time |
| silver.supplier | ingestion_at | CURRENT_TIMESTAMP at pipeline run time |
| silver.samplers | ingestion_at | CURRENT_TIMESTAMP at pipeline run time |
| silver.mineral | ingestion_at | CURRENT_TIMESTAMP at pipeline run time |
| silver.locations | ingestion_at | CURRENT_TIMESTAMP at pipeline run time |

---

## Design Decisions
| Decision | Choice | Reason |
|---|---|---|
| ID columns stay VARCHAR | VARCHAR | AppSheet IDs are hash strings, not integers |
| Dates converted from serial | DATE | Excel stores dates as floats, unusable without conversion |
| Phone numbers standardised | +234XXXXXXXXXX | Inconsistent formats across source data |
| #ERROR! replaced with NULL | NULL | Keeps data usable without losing the row |
| ingestion_at added | TIMESTAMP | Allows pipeline run tracking and debugging |
| Photo columns dropped | Dropped | URLs are not needed for analytics |
| task_helper column dropped | Dropped | Internal AppSheet helper column, no analytical value |