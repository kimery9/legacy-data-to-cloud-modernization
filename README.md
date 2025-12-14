# ⭐ Legacy Data Modernization Pipeline — End-to-End Project

This project demonstrates a complete **modern data engineering workflow**, starting from a **legacy OLTP database** and ending in a **cloud-hosted analytics warehouse** modeled using a **star schema**.

It simulates real-world modernization work where companies migrate siloed legacy systems into scalable, cloud-native analytics platforms.

---

# 🚀 Architecture Overview

This pipeline performs:

1. **Extraction** from a legacy SQLite database  
2. **Landing** raw data into **Azure Blob Storage** (Bronze layer)  
3. **Transformation** into a clean dataset using Python (Silver layer)  
4. **Loading** curated data into **Snowflake** using external stages  
5. **Modeling** a dimensional **star schema** (Gold layer)  
6. **Building analytics-ready FACT and DIMENSION tables**

---

# 🧱 1. Legacy Source System — SQLite (OLTP)

The project begins with the **Chinook** SQLite database, representing a typical legacy operational system:

- Highly normalized schema  
- Not optimized for analytics  
- Requires transformation before use  
- Provides multiple relational tables:
  - `Customer`
  - `Invoice`
  - `InvoiceLine`
  - `Track`
  - `Album`
  - `Artist`

---

# ☁️ 2. Data Lake Landing — Azure Blob Storage (Bronze Layer)

A Python ingestion script extracts raw tables from SQLite, converts them into Parquet files, and stores them in Azure Blob Storage.

### Why Parquet?

- Columnar  
- Highly compressed  
- Analytics-optimized  
- Schema-on-read  

The **Bronze layer** serves as immutable raw storage.

---

# 🥈 3. Transformation Layer — Python (Silver Layer)

A Python script cleans, standardizes, and enriches the data into a single curated dataset:  
`user_events.parquet`

Transformations include:

- Joining InvoiceLine + Invoice + Customer + Track + Album + Artist  
- Standardizing column names  
- Fixing data types  
- Normalizing timestamps  
- Adding derived metrics:
  - `event_type`
  - `event_date`
  - `session_id`
  - `total_amount`
  - `is_trial_user`

Output is written to:

```
data_lake/cleaned/user_events/<run_date>/user_events.parquet
```

---

# ❄️ 4. Load Into Snowflake — External Stage + Storage Integration

Snowflake loads cleaned Parquet data directly from Azure Blob Storage using:

- Storage Integration (Snowflake ↔ Azure trust)
- External Stage (`@CLEANED_STAGE`)
- Staging table (`FACT_USER_EVENTS_STG`)
- Explicit casting into typed fact table

This follows production-grade, schema-safe loading patterns.

---

# 🥇 5. Analytical Warehouse (Gold Layer) — Star Schema

We model the warehouse using dimensional modeling, creating:

- **1 Fact Table** — `FACT_USER_EVENTS`
- **3 Dimension Tables** — `DIM_USER`, `DIM_DATE`, `DIM_TRACK`

---

# 🌟 Star Schema Diagram

```
                 DIM_DATE
                 (date_key)
                     |
                     |
DIM_USER ------- FACT_USER_EVENTS ------- DIM_TRACK
(user_id)           (event grain)        (track_id)
```

---

# 📊 FACT Table — `FACT_USER_EVENTS`

- Grain: one row per invoice line event  
- Keys: `user_id`, `track_id`, `album_id`, `artist_id`, `event_date`  
- Measures: `total_amount`, `quantity`, `unit_price`  
- Timestamps: `invoice_date`, `event_date`  
- Attributes: `country`, `city`, `session_id`, `is_trial_user`  

---

# 👤 DIM_USER

Aggregated, user-level attributes:

- First/last activity  
- Total events  
- Total spend  
- Avg order value  
- Country, city  
- Trial status  

---

# 📅 DIM_DATE

A generated date spine with:

- Year, month, day  
- Day names  
- Week of year  
- Month names  
- Weekend indicator  

---

# 🎵 DIM_TRACK

Content-level performance metrics:

- First/last event date  
- Total events  
- Total orders  
- Total revenue  
- Average revenue per event  

---

# 📈 Example Analytics Queries

Revenue by month & country, top users, weekend/weekday trends, and catalog performance analysis are all supported through the star schema.

---

# 🧪 Tech Stack

| Layer | Technologies |
|-------|--------------|
| Source | SQLite (OLTP) |
| Ingestion | Python, pandas, pyarrow |
| Storage | Azure Blob Storage (Parquet) |
| Warehouse | Snowflake |
| Modeling | SQL, Dimensional Modeling |
| Compute | Snowflake Warehouse |

---

# 🎯 Skills Demonstrated

- Cloud data pipelines (Azure + Snowflake)  
- ETL/ELT design  
- Dimensional modeling (star schema)  
- Data lakehouse architecture  
- Python & SQL data engineering  
- Secure cloud integrations  
- Analytics engineering foundations  

---
