# Bronze Schema Design
## Minerals ETL Pipeline

---

## Overview
Bronze tables store raw data exactly as it comes from the source Excel file.
Column names follow snake_case convention. Data types are kept as broad as 
possible to avoid rejecting dirty data. All columns are nullable.

---

## bronze.sampling_requests
Source: Sampling Requests Table

| Column Name | Data Type | Source Column |
|---|---|---|
| scm_email | VARCHAR | SCM Email |
| scm_name | VARCHAR | SCM Name |
| request_id | VARCHAR | Request_ID (PK) |
| mineral_type_id | VARCHAR | Mineral_Type_ID |
| test_type_id | VARCHAR | Test_Type_ID (FK) |
| supplier_id | VARCHAR | Supplier_ID (FK) |
| supplier_phone | VARCHAR | Supplier Phone |
| pickup_location_id | VARCHAR | Pickup_Location_ID |
| state | VARCHAR | State |
| region | VARCHAR | Region |
| created_at | VARCHAR | Created_At |
| request_status | VARCHAR | Request_Status |
| status_updated_at | VARCHAR | Status_Updated_At |
| comments | VARCHAR | Comments |
| next_step | VARCHAR | Next Step |

---

## bronze.task_assignment
Source: Task Assignment Table

| Column Name | Data Type | Source Column |
|---|---|---|
| task_helper | VARCHAR | Task Helper |
| task_id | VARCHAR | Task_ID (PK) |
| request_id | VARCHAR | Request_ID (FK) |
| request_created_date | VARCHAR | Request Created Date |
| mineral_type_id | VARCHAR | Mineral_Type_ID |
| supplier_id | VARCHAR | Supplier_ID |
| supplier_phone | VARCHAR | Supplier Phone |
| pickup_location | VARCHAR | Pickup Location |
| state | VARCHAR | State |
| region | VARCHAR | Region |
| assigned_sampler_id | VARCHAR | Assigned_Sampler_ID (FK) |
| lab_id | VARCHAR | Lab_ID (FK) |
| task_status | VARCHAR | Task_Status |
| sampler_acceptance_status | VARCHAR | Sampler Acceptance Status |
| status_updated_at | VARCHAR | Status_Updated_At |
| assigned_by | VARCHAR | Assigned_By (FK) |
| assigned_at | VARCHAR | Assigned_At |

---

## bronze.sample_collection
Source: Sample Collection Table

| Column Name | Data Type | Source Column |
|---|---|---|
| collection_id | VARCHAR | Collection_ID (PK) |
| task_id | VARCHAR | Task_ID (FK) |
| request_id | VARCHAR | Request_ID (FK) |
| sampler_id | VARCHAR | Sampler_ID (FK) |
| sampler_email | VARCHAR | Sampler Email |
| supplier_id | VARCHAR | Supplier_ID (FK) |
| supplier_phone | VARCHAR | Supplier Phone |
| pickup_location_id | VARCHAR | Pickup_Location_ID (FK) |
| collection_acceptance_status | VARCHAR | Collection_Acceptance_Status |
| collection_acceptance_datetime | VARCHAR | Collection_Acceptance Datetime |
| mine_gate_photo | VARCHAR | Mine_Gate_Photo |
| mine_gate_photo_url | VARCHAR | Mine_Gate_Photo_url |
| sample_photo | VARCHAR | Sample_Photo |
| sample_photo_url | VARCHAR | Sample_Photo_url |
| collection_date | VARCHAR | Collection_Date |
| collection_status | VARCHAR | Collection_Status |
| collection_status_updated_at | VARCHAR | Collection_Status_Updated_At |
| driver_id | VARCHAR | Driver_ID (FK) |
| dispatch_date | VARCHAR | Dispatch_Date |
| dispatch_status | VARCHAR | Dispatch_Status |
| dispatch_status_updated_at | VARCHAR | Dispatch_Status_Updated_At |
| bot_record_id | VARCHAR | Bot_Record_ID |

---

## bronze.lab_test_results
Source: Lab Test Results Table

| Column Name | Data Type | Source Column |
|---|---|---|
| test_result_id | VARCHAR | Test_Result_ID (PK) |
| collection_id | VARCHAR | Collection_ID (FK) |
| task_id | VARCHAR | Task_ID (FK) |
| request_id | VARCHAR | Request_ID (FK) |
| sampler_id | VARCHAR | Sampler_ID (FK) |
| driver_id | VARCHAR | Driver_ID (FK) |
| result_document | VARCHAR | Result_Document |
| result_document_url | VARCHAR | Result_Document_url |
| uploaded_by | VARCHAR | Uploaded_By |
| uploader_name | VARCHAR | Uploader Name |
| result_received_date | VARCHAR | Result_Received_Date |
| analysis_status | VARCHAR | Analysis_Status |
| status_updated_at | VARCHAR | Status_Updated_At |
| was_sample_received | VARCHAR | Was the Sample Received? |

---

## bronze.supplier
Source: Supplier Table

| Column Name | Data Type | Source Column |
|---|---|---|
| supplier_id | VARCHAR | Supplier_ID (PK) |
| supplier_name | VARCHAR | Supplier_Name |
| phone_number | VARCHAR | Phone_Number |

---

## bronze.samplers
Source: Samplers Table

| Column Name | Data Type | Source Column |
|---|---|---|
| sampler_id | VARCHAR | Sampler_ID (PK) |
| full_name | VARCHAR | Full_Name |
| phone_number | VARCHAR | Phone_Number |
| state_assigned | VARCHAR | State_Assigned |
| region_assigned | VARCHAR | Region_Assigned |
| email | VARCHAR | Email |

---

## bronze.mineral
Source: Mineral Table

| Column Name | Data Type | Source Column |
|---|---|---|
| mineral_type_id | VARCHAR | Mineral_Type_ID (PK) |
| mineral_name | VARCHAR | Mineral_Name |

---

## bronze.locations
Source: Locations Table

| Column Name | Data Type | Source Column |
|---|---|---|
| location_id | VARCHAR | Location_ID (PK) |
| state_name | VARCHAR | State_Name |
| region_name | VARCHAR | Region_Name |

---

## Design Decisions
| Decision | Choice | Reason |
|---|---|---|
| All columns VARCHAR | VARCHAR | Bronze ingests raw data as-is, casting happens in Silver |
| All columns nullable | NULL | We never want Bronze to reject records due to missing data |
| Date columns as VARCHAR | VARCHAR | Source dates are Excel serial numbers, conversion happens in Silver |
| Column names | snake_case | Follows project naming conventions |