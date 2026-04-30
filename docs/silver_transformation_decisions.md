# Silver Transformation Decisions
## Minerals ETL Pipeline

---

## silver.sampling_requests

| Column | Issue | Decision | Reason |
|---|---|---|---|
| created_at | Datetime string | Cast to DATE | Time component not needed |
| status_updated_at | Datetime string | Cast to DATE | Time component not needed |
| comments | 15 x #ERROR! | Replace with NULL | AppSheet formula error, not real data |
| supplier_phone | Inconsistent formats, Unicode chars | Standardise to +234XXXXXXXXXX | Needed for joins and readability |
| scm_email | Mixed case | Lowercase and trim | Consistency |
| scm_name | Whitespace | Trim | Consistency |

---

## silver.task_assignment

| Column | Issue | Decision | Reason |
|---|---|---|---|
| assigned_at | Datetime string | Cast to DATE | Time component not needed |
| request_created_date | Datetime string | Cast to DATE | Time component not needed |
| status_updated_at | Datetime string | Cast to DATE | Time component not needed |
| sampler_acceptance_status | 420 "None" strings | Leave as-is | MNAR — sampler never asked or status never updated |
| assigned_sampler_id | 23 nulls | Leave as-is | MNAR — tasks not yet assigned |
| task_helper | AppSheet internal column | Drop | No analytical value |
| supplier_phone | Inconsistent formats | Standardise to +234XXXXXXXXXX | Consistency |

---

## silver.sample_collection

| Column | Issue | Decision | Reason |
|---|---|---|---|
| collection_acceptance_status | 2 rows contain datetime values | Replace with NULL | AppSheet data entry error |
| supplier_phone | 89 x #ERROR!, Unicode chars | Replace errors with NULL, strip Unicode | AppSheet formula error |
| collection_date | 331 nulls | Leave as-is | MNAR — collection not happened yet |
| dispatch_date | 341 nulls | Leave as-is | MNAR — not dispatched yet |
| collection_status_updated_at | 460 nulls (all null) | Drop column | No information |
| dispatch_status_updated_at | 460 nulls (all null) | Drop column | No information |
| mine_gate_photo | Photo data | Drop | Not needed for analytics |
| mine_gate_photo_url | URL | Drop | Not needed for analytics |
| sample_photo | Photo data | Drop | Not needed for analytics |
| sample_photo_url | URL | Drop | Not needed for analytics |
| collection_date | Datetime string | Cast to DATE | Time component not needed |
| dispatch_date | Datetime string | Cast to DATE | Time component not needed |
| collection_acceptance_datetime | Datetime string | Cast to TIMESTAMP | Time component meaningful here |
| sampler_email | Mixed case | Lowercase and trim | Consistency |

---

## silver.lab_test_results

| Column | Issue | Decision | Reason |
|---|---|---|---|
| result_received_date | 133 nulls | Leave as-is | MNAR — lab not processed yet |
| analysis_status | 133 nulls | Leave as-is | MNAR — lab not processed yet |
| was_sample_received | 133 nulls | Leave as-is | MNAR — lab not processed yet |
| result_document | 133 nulls | Drop column | No analytical value |
| result_document_url | 133 nulls | Drop column | No analytical value |
| uploader_name | 132 nulls | Drop column | uploaded_by already captures this |
| result_received_date | Datetime string | Cast to DATE | Time component not needed |
| status_updated_at | Datetime string | Cast to DATE | Time component not needed |
| uploaded_by | Mixed case | Lowercase and trim | Consistency |

---

## silver.supplier

| Column | Issue | Decision | Reason |
|---|---|---|---|
| supplier_name | Duplicate names with different IDs | Keep as-is | Different suppliers, ID is unique key |
| supplier_name | Whitespace | Trim and title case | Consistency |
| phone_number | Inconsistent formats, Unicode chars | Standardise to +234XXXXXXXXXX | Consistency |

---

## silver.samplers

| Column | Issue | Decision | Reason |
|---|---|---|---|
| region_assigned | 15 nulls, 2 hash IDs | Set all to NULL | Hash IDs are unresolved AppSheet relational fields, meaningless |
| full_name | Whitespace | Trim and title case | Consistency |
| phone_number | Inconsistent formats | Standardise to +234XXXXXXXXXX | Consistency |
| email | Mixed case | Lowercase and trim | Consistency |

---

## silver.mineral

| Column | Issue | Decision | Reason |
|---|---|---|---|
| mineral_name | "Test" record | Filter out | Test record from AppSheet setup |
| mineral_name | Whitespace | Trim and title case | Consistency |

---

## silver.locations

| Column | Issue | Decision | Reason |
|---|---|---|---|
| state_name | Whitespace | Trim and title case | Consistency |
| region_name | Whitespace | Trim and title case | Consistency |

---

## Phone Number Standardisation Rules
All phone numbers will be standardised to +234XXXXXXXXXX format using 
these rules applied in sequence:
1. Strip all Unicode control characters (U+202A, U+202C, U+00A0 etc.)
2. Remove all spaces, hyphens, brackets
3. If starts with 0 — replace leading 0 with +234
4. If starts with 234 — add + prefix
5. If starts with +234(0) — remove the (0)
6. If already +234XXXXXXXXXX — leave as-is
7. If result is not 14 characters (+234XXXXXXXXXX) — set to NULL