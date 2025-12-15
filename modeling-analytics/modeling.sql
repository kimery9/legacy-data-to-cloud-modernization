CREATE DATABASE IF NOT EXISTS LEGACY_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS LEGACY_ANALYTICS.CLEAN;

USE DATABASE LEGACY_ANALYTICS;
USE SCHEMA CLEAN;

CREATE OR REPLACE WAREHOUSE WH_ANALYTICS
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE WAREHOUSE WH_ANALYTICS;

CREATE OR REPLACE STORAGE INTEGRATION AZURE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = 'your-tenant-id'
  STORAGE_ALLOWED_LOCATIONS = ('azure://datalakemodern.blob.core.windows.net/data-lake');

DESC STORAGE INTEGRATION AZURE_INT;

CREATE OR REPLACE STAGE CLEANED_STAGE
  URL='azure://datalakemodern.blob.core.windows.net/data-lake/cleaned'
  STORAGE_INTEGRATION = AZURE_INT
  FILE_FORMAT = (TYPE = PARQUET);

CREATE OR REPLACE TABLE FACT_USER_EVENTS (
    event_id        NUMBER,
    user_id         NUMBER,
    event_type      STRING,
    track_id        NUMBER,
    album_id        NUMBER,
    artist_id       NUMBER,
    unit_price      FLOAT,
    quantity        NUMBER,
    total_amount    FLOAT,
    invoice_id      NUMBER,
    invoice_date    TIMESTAMP_NTZ,
    event_date      DATE,
    country         STRING,
    city            STRING,
    session_id      STRING,
    is_trial_user   BOOLEAN
);


COPY INTO FACT_USER_EVENTS
FROM @CLEANED_STAGE/user_events/2025-12-13/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';


SELECT COUNT(*) FROM FACT_USER_EVENTS;
SELECT * FROM FACT_USER_EVENTS LIMIT 20

CREATE OR REPLACE TABLE DIM_USER (
    user_id           NUMBER,       -- natural key from source
    first_event_date  DATE,
    last_event_date   DATE,
    total_events      NUMBER,
    total_orders      NUMBER,
    total_spend       FLOAT,
    avg_order_value   FLOAT,
    country           STRING,
    city              STRING,
    is_trial_user     BOOLEAN
);

INSERT INTO DIM_USER (
    user_id,
    first_event_date,
    last_event_date,
    total_events,
    total_orders,
    total_spend,
    avg_order_value,
    country,
    city,
    is_trial_user
)
SELECT
    user_id,
    MIN(event_date)                         AS first_event_date,
    MAX(event_date)                         AS last_event_date,
    COUNT(*)                                AS total_events,
    COUNT(DISTINCT invoice_id)              AS total_orders,
    SUM(total_amount)                       AS total_spend,
    AVG(total_amount)                       AS avg_order_value,
    MAX(country)                            AS country,     -- last-known / arbitrary
    MAX(city)                               AS city,
    MAX(is_trial_user)                      AS is_trial_user
FROM FACT_USER_EVENTS
GROUP BY user_id;

--create date dimension with date spine for completeness of time analysis
CREATE OR REPLACE TABLE DIM_DATE AS
WITH bounds AS (
    SELECT 
        MIN(event_date) AS min_date,
        MAX(event_date) AS max_date
    FROM FACT_USER_EVENTS
),
nums AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS n
    FROM TABLE(GENERATOR(ROWCOUNT => 50000))  
),
date_spine AS (
    SELECT 
        DATEADD(day, n, min_date) AS date_key
    FROM bounds, nums
    WHERE DATEADD(day, n, min_date) <= (SELECT max_date FROM bounds)
)
SELECT
    date_key,
    YEAR(date_key)                      AS year,
    MONTH(date_key)                     AS month,
    DAY(date_key)                       AS day,
    TO_CHAR(date_key, 'DAY')            AS day_name,
    TO_CHAR(date_key, 'DY')             AS day_name_short,
    WEEKOFYEAR(date_key)                AS week_of_year,
    TO_CHAR(date_key, 'MONTH')          AS month_name,
    TO_CHAR(date_key, 'MON')            AS month_short,
    CASE 
        WHEN DAYOFWEEK(date_key) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END                                 AS is_weekend
FROM date_spine
ORDER BY date_key;

USE DATABASE LEGACY_ANALYTICS;
USE SCHEMA CLEAN;
USE WAREHOUSE WH_ANALYTICS;

CREATE OR REPLACE TABLE DIM_TRACK AS
SELECT
    track_id,                      -- FK from fact
    album_id,
    artist_id,
    MIN(event_date)        AS first_event_date,
    MAX(event_date)        AS last_event_date,
    COUNT(*)               AS total_events,
    COUNT(DISTINCT invoice_id) AS total_orders,
    SUM(total_amount)      AS total_revenue,
    AVG(total_amount)      AS avg_line_revenue
FROM FACT_USER_EVENTS
GROUP BY
    track_id,
    album_id,
    artist_id;


