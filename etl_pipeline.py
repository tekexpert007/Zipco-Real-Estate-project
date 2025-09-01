# etl_pipeline.py
import subprocess
import datetime

print(f"ETL run started: {datetime.datetime.now()}")

# Run your Bronze -> Silver -> Gold logic
# Example: call your API and save JSON
# transform and save parquet/Delta
# etc.

print("ETL process completed successfully.")
