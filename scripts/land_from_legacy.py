from pathlib import Path
import sqlite3
import pandas as pd
from datetime import date

DB_PATH = "legacy-db/Chinook_Sqlite.sqlite"
LANDING_ROOT = Path("data_lake")

run_date = date.today().strftime("%Y-%m-%d")

conn = sqlite3.connect(DB_PATH)

# get non-system tables
tables = pd.read_sql(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';",
    conn,
)

for table in tables["name"]:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)

    # partitioned path: data_lake/raw/<table>/<run_date>/<table>.parquet
    outdir = LANDING_ROOT / "raw" / table / run_date
    outdir.mkdir(parents=True, exist_ok=True)

    # choose parquet (modern lake format); you can also do CSV if you want
    df.to_parquet(outdir / f"{table}.parquet", index=False)

conn.close()

print("Landing complete ✅")
