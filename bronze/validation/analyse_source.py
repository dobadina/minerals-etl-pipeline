import pandas as pd
import os

SOURCE_FILE = "data/source/minerals_appsheet_data_model.xlsx"

SHEETS_IN_SCOPE = {
    "sampling_requests": "Sampling Requests Table",
    "task_assignment": "Task Assignment Table",
    "sample_collection": "Sample Collection Table",
    "lab_test_results": "Lab Test Results Table",
    "supplier": "Supplier Table",
    "samplers": "Samplers Table",
    "mineral": "Mineral Table",
    "locations": "Locations Table"
}

def analyse_sheet(name, sheet_name, df):
    print(f"\n{'='*60}")
    print(f"TABLE: {name} (source: {sheet_name})")
    print(f"{'='*60}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"\n--- Column Profile ---")
    for col in df.columns:
        null_count = df[col].isna().sum()
        error_count = df[col].astype(str).str.contains('#ERROR!', na=False).sum()
        unique_count = df[col].nunique()
        dtype = df[col].dtype
        print(f"  {col}")
        print(f"    dtype: {dtype} | nulls: {null_count} | errors: {error_count} | unique: {unique_count}")

def main():
    print(f"Source file: {SOURCE_FILE}")
    print(f"File exists: {os.path.exists(SOURCE_FILE)}")

    for name, sheet_name in SHEETS_IN_SCOPE.items():
        df = pd.read_excel(SOURCE_FILE, sheet_name=sheet_name, dtype=str)
        analyse_sheet(name, sheet_name, df)

    print(f"\n{'='*60}")
    print("Analysis complete")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()