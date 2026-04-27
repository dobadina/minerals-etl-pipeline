# Minerals ETL Pipeline 

An end-to-end ETL pipeline for mineral sampling and quality assurance data 
built using Python, DuckDB, Prefect and Power BI Desktop.

## Architecture
Medallion architecture with three layers:
- **Bronze** — raw data ingested as-is from source Excel file
- **Silver** — cleaned, standardised and enriched data
- **Gold** — star schema views for Power BI reporting

## Stack
| Component | Tool |
|-----------|------|
| Database | DuckDB |
| Transformation | Python + Pandas |
| Orchestration | Prefect |
| Reporting | Power BI Desktop |
| Version Control | Git + GitHub |

## Project Structure
minerals_etl_pipeline/
├── data/
│   ├── source/          # Raw source Excel files
│   └── logs/            # ETL log files
├── bronze/
│   ├── ingestion/       # Bronze ingestion scripts
│   └── validation/      # Bronze validation scripts
├── silver/
│   ├── transformations/ # Silver transformation scripts
│   └── validation/      # Silver validation scripts
├── gold/
│   ├── models/          # Gold dimension and fact scripts
│   └── validation/      # Gold validation scripts
├── utils/               # Shared utility functions
├── docs/                # Documentation and diagrams
├── tests/               # Test scripts
├── requirements.txt
└── README.md

## Setup
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```