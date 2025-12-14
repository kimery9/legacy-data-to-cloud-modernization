from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
parquet_path = ROOT / "data_lake" / "cleaned" / "user_events" / "2025-12-13" / "user_events.parquet"

print("Loading:", parquet_path)
df = pd.read_parquet(parquet_path)
print("Columns:", df.columns.tolist())
print(df.head(10)[["event_id", "user_id", "invoice_id", "invoice_date"]])
print(df.dtypes)
