# Medallion Architecture Design
## Minerals ETL Pipeline

---

## Overview
This pipeline follows a Medallion architecture with three layers stored in a 
single DuckDB database file. Each layer has a dedicated schema. Data flows 
sequentially from Bronze to Silver to Gold — no layer reads from a layer 
above it.

---

## Data Flow
Source (Excel) → Bronze (raw) → Silver (clean) → Gold (star schema) → Power BI

---

## Source Data
- **Format:** Excel (.xlsx)
- **Location:** data/source/
- **Tables in scope:** 8 (sampling focused)
    - Sampling Requests
    - Task Assignment
    - Sample Collection
    - Lab Test Results
    - Supplier
    - Samplers
    - Mineral
    - Locations

---

## Bronze Layer
- **Schema:** bronze
- **Purpose:** Store raw data exactly as it comes from the source
- **Load method:** Full load — truncate and insert on every run
- **Transformations:** None — data lands as-is including errors and nulls
- **Object type:** Tables
- **Naming:** bronze.[source_table_name] e.g. bronze.sampling_requests
- **Target audience:** Data Engineers

### Tables
| Bronze Table | Source Sheet |
|---|---|
| bronze.sampling_requests | Sampling Requests Table |
| bronze.task_assignment | Task Assignment Table |
| bronze.sample_collection | Sample Collection Table |
| bronze.lab_test_results | Lab Test Results Table |
| bronze.supplier | Supplier Table |
| bronze.samplers | Samplers Table |
| bronze.mineral | Mineral Table |
| bronze.locations | Locations Table |

---

## Silver Layer
- **Schema:** silver
- **Purpose:** Clean, standardise and enrich Bronze data ready for analysis
- **Load method:** Full load — truncate and insert on every run
- **Transformations:**
    - Resolve #ERROR! values
    - Convert Excel serial dates to proper dates
    - Standardise phone number formats
    - Standardise text fields (trim whitespace, consistent casing)
    - Derive analytical columns (e.g. request_to_dispatch_days)
    - Remove duplicates
- **Object type:** Tables
- **Naming:** silver.[table_name] e.g. silver.sampling_requests
- **Target audience:** Data Analysts and Data Engineers

### Tables
| Silver Table | Source Bronze Table |
|---|---|
| silver.sampling_requests | bronze.sampling_requests |
| silver.task_assignment | bronze.task_assignment |
| silver.sample_collection | bronze.sample_collection |
| silver.lab_test_results | bronze.lab_test_results |
| silver.supplier | bronze.supplier |
| silver.samplers | bronze.samplers |
| silver.mineral | bronze.mineral |
| silver.locations | bronze.locations |

---

## Gold Layer
- **Schema:** gold
- **Purpose:** Star schema modelled for Power BI reporting
- **Load method:** Full load — truncate and insert on every run
- **Transformations:**
    - Integrate data from multiple Silver tables
    - Apply business logic and aggregations
    - Rename columns to business-friendly names
- **Object type:** Tables
- **Naming:** gold.fact_[name], gold.dim_[name]
- **Target audience:** Business users and Data Analysts

### Star Schema
| Object | Type | Description |
|---|---|---|
| gold.fact_sampling_requests | Fact | One row per sampling request, contains all measurable events and foreign keys to dimensions |
| gold.dim_supplier | Dimension | Supplier details |
| gold.dim_sampler | Dimension | Sampler details |
| gold.dim_mineral | Dimension | Mineral type details |
| gold.dim_region | Dimension | State and region details |
| gold.dim_date | Dimension | Date dimension for time intelligence |

---

## Database
- **Engine:** DuckDB
- **File location:** data/minerals_etl.duckdb
- **Schemas:** bronze, silver, gold

---

## Pipeline Execution Order
1. Run Bronze ingestion scripts
2. Run Bronze validation
3. Run Silver transformation scripts
4. Run Silver validation
5. Run Gold model scripts
6. Run Gold validation

---

## Key Decisions
| Decision | Choice | Reason |
|---|---|---|
| Scope | Sampling only (8 tables) | Business questions are sampling focused, logistics tables are thin |
| Load strategy | Full load at all layers | Data volume is small, simplicity over complexity |
| Database | DuckDB | Local, file-based, no server required, full SQL support |
| Gold objects | Tables not views | Better Power BI performance with Import mode |