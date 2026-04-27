# Data Flow Diagram
## Minerals ETL Pipeline

---

```mermaid
flowchart TD
    subgraph SOURCE["📁 Source"]
        A[Excel File\nMinerals_Appsheet_Data_Model.xlsx]
    end

    subgraph BRONZE["🥉 Bronze Layer — DuckDB Schema: bronze"]
        B1[bronze.sampling_requests]
        B2[bronze.task_assignment]
        B3[bronze.sample_collection]
        B4[bronze.lab_test_results]
        B5[bronze.supplier]
        B6[bronze.samplers]
        B7[bronze.mineral]
        B8[bronze.locations]
    end

    subgraph SILVER["🥈 Silver Layer — DuckDB Schema: silver"]
        S1[silver.sampling_requests]
        S2[silver.task_assignment]
        S3[silver.sample_collection]
        S4[silver.lab_test_results]
        S5[silver.supplier]
        S6[silver.samplers]
        S7[silver.mineral]
        S8[silver.locations]
    end

    subgraph GOLD["🥇 Gold Layer — DuckDB Schema: gold"]
        G1[gold.dim_supplier]
        G2[gold.dim_sampler]
        G3[gold.dim_mineral]
        G4[gold.dim_region]
        G5[gold.dim_date]
        G6[gold.fact_sampling_requests]
    end

    subgraph REPORTING["📊 Reporting"]
        P[Power BI Desktop]
    end

    A -->|Full load\nTruncate & Insert| B1
    A -->|Full load\nTruncate & Insert| B2
    A -->|Full load\nTruncate & Insert| B3
    A -->|Full load\nTruncate & Insert| B4
    A -->|Full load\nTruncate & Insert| B5
    A -->|Full load\nTruncate & Insert| B6
    A -->|Full load\nTruncate & Insert| B7
    A -->|Full load\nTruncate & Insert| B8

    B1 -->|Clean & Standardise| S1
    B2 -->|Clean & Standardise| S2
    B3 -->|Clean & Standardise| S3
    B4 -->|Clean & Standardise| S4
    B5 -->|Clean & Standardise| S5
    B6 -->|Clean & Standardise| S6
    B7 -->|Clean & Standardise| S7
    B8 -->|Clean & Standardise| S8

    S5 -->|Surrogate Key| G1
    S6 -->|Surrogate Key| G2
    S7 -->|Surrogate Key| G3
    S8 -->|Surrogate Key| G4
    S1 -->|Date Range| G5

    S1 -->|Join| G6
    S2 -->|Join| G6
    S3 -->|Join| G6
    S4 -->|Join| G6
    G1 -->|FK| G6
    G2 -->|FK| G6
    G3 -->|FK| G6
    G4 -->|FK| G6
    G5 -->|FK| G6

    G6 --> P
    G1 --> P
    G2 --> P
    G3 --> P
    G4 --> P
    G5 --> P
```

---

## Pipeline Execution Order

```
1. pipeline_bronze.py
      ├── load_bronze_sampling_requests.py
      ├── load_bronze_task_assignment.py
      ├── load_bronze_sample_collection.py
      ├── load_bronze_lab_test_results.py
      ├── load_bronze_supplier.py
      ├── load_bronze_samplers.py
      ├── load_bronze_mineral.py
      └── load_bronze_locations.py

2. pipeline_silver.py
      ├── load_silver_sampling_requests.py
      ├── load_silver_task_assignment.py
      ├── load_silver_sample_collection.py
      ├── load_silver_lab_test_results.py
      ├── load_silver_supplier.py
      ├── load_silver_samplers.py
      ├── load_silver_mineral.py
      └── load_silver_locations.py

3. pipeline_gold.py
      ├── load_gold_dim_supplier.py
      ├── load_gold_dim_sampler.py
      ├── load_gold_dim_mineral.py
      ├── load_gold_dim_region.py
      ├── load_gold_dim_date.py
      └── load_gold_fact_sampling_requests.py
```