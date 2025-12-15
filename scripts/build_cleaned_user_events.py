from pathlib import Path
import sqlite3
import pandas as pd
from datetime import date

# Resolve paths from project root
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "legacy-db" / "Chinook_Sqlite.sqlite"
CLEANED_ROOT = ROOT / "data_lake" / "cleaned"

run_date = date.today().strftime("%Y-%m-%d")

# --- Connect to legacy DB ---
if not DB_PATH.exists():
    raise FileNotFoundError(f"Legacy DB not found at {DB_PATH}")

conn = sqlite3.connect(DB_PATH)

# --- Load source tables from legacy DB ---
customer = pd.read_sql("SELECT * FROM Customer", conn)
invoice = pd.read_sql("SELECT * FROM Invoice", conn)
invoice_line = pd.read_sql("SELECT * FROM InvoiceLine", conn)
track = pd.read_sql("SELECT * FROM Track", conn)
album = pd.read_sql("SELECT * FROM Album", conn)
artist = pd.read_sql("SELECT * FROM Artist", conn)

conn.close()

# --- Join into a user_events frame into denorm view ---
df = (
    invoice_line
    .merge(invoice, on="InvoiceId", how="left", suffixes=("", "_invoice"))
    .merge(customer, on="CustomerId", how="left", suffixes=("", "_customer"))
    .merge(track, on="TrackId", how="left")
    .merge(album, on="AlbumId", how="left")
    .merge(artist, on="ArtistId", how="left")
)

# Standardize column names
df.columns = [c.lower() for c in df.columns]

# --- Choose correct unit price column (from invoice_line) ---
price_cols = [c for c in df.columns if "unitprice" in c]
if not price_cols:
    raise KeyError(f"No unitprice-like column found. Columns: {df.columns.tolist()}")

# Heuristic: prefer the first one (typically unitprice from InvoiceLine)
base_price_col = price_cols[0]

# --- Clean & derive fields ---
# quantity: fill missing with 1
df["quantity"] = df.get("quantity", 1).fillna(1)

# invoice date → timestamp
df["invoicedate"] = pd.to_datetime(df["invoicedate"], errors="coerce")
df["event_date"] = df["invoicedate"].dt.date

# simple event_type
df["event_type"] = "purchase"

# session_id: pretend each invoice is one session
df["session_id"] = df["invoiceid"].astype(str) + "-" + df["customerid"].astype(str)

# total amount per line using chosen price column
df["total_amount"] = df["quantity"] * df[base_price_col].astype(float)

# lifetime spend per customer (for is_trial_user)
user_spend = df.groupby("customerid")["total_amount"].transform("sum")
df["is_trial_user"] = user_spend < 5  # fake business rule

# --- Select cleaned columns for warehouse and analytical schema ---
user_events = df[
    [
        "invoicelineid",       # event_id
        "customerid",          # user_id
        "event_type",
        "trackid",
        "albumid",
        "artistid",
        base_price_col,        # unitprice_x or similar
        "quantity",
        "total_amount",
        "invoiceid",
        "invoicedate",
        "event_date",
        "country",
        "city",
        "session_id",
        "is_trial_user",
    ]
].rename(
    columns={
        "invoicelineid": "event_id",
        "customerid": "user_id",
        "trackid": "track_id",
        "albumid": "album_id",
        "artistid": "artist_id",
        base_price_col: "unit_price",
        "invoiceid": "invoice_id",   
        "invoicedate": "invoice_date",
    }
)

# Normalize date types in the silver layer
user_events["invoice_date"] = pd.to_datetime(
    user_events["invoice_date"], errors="coerce"
)
user_events["event_date"] = user_events["invoice_date"].dt.date

# Store as ISO strings so Snowflake can parse cleanly
user_events["invoice_date"] = user_events["invoice_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
user_events["event_date"] = pd.to_datetime(
    user_events["event_date"], errors="coerce"
).dt.strftime("%Y-%m-%d")

# --- Write cleaned user_events parquet ---
outdir = CLEANED_ROOT / "user_events" / run_date
outdir.mkdir(parents=True, exist_ok=True)
user_events.to_parquet(outdir / "user_events.parquet", index=False)

print(f"Wrote cleaned user_events to {outdir}")
