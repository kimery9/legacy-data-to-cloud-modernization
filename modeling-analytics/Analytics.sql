USE DATABASE LEGACY_ANALYTICS;
USE SCHEMA CLEAN;

--Revenue by country and motn
SELECT
    d.year,
    d.month,
    d.month_name,
    u.country,
    SUM(f.total_amount) AS revenue
FROM FACT_USER_EVENTS f
JOIN DIM_USER u
  ON f.user_id = u.user_id
JOIN DIM_DATE d
  ON f.event_date = d.date_key
GROUP BY
    d.year, d.month, d.month_name, u.country
ORDER BY
    d.year, d.month, revenue DESC;


--top 10 by total spend
SELECT
    u.user_id,
    u.country,
    u.city,
    u.total_spend
FROM DIM_USER u
ORDER BY u.total_spend DESC
LIMIT 10;

-- weekend vs weekday revenue 
SELECT
    d.is_weekend,
    SUM(f.total_amount) AS revenue,
    COUNT(*) AS events
FROM FACT_USER_EVENTS f
JOIN DIM_DATE d
  ON f.event_date = d.date_key
GROUP BY d.is_weekend;

--top 10 tracks by revenue
SELECT
    t.track_id,
    t.album_id,
    t.artist_id,
    t.total_revenue,
    t.total_events
FROM DIM_TRACK t
ORDER BY t.total_revenue DESC
LIMIT 10;

SELECT
    d.year,
    d.month,
    t.track_id,
    SUM(f.total_amount) AS revenue
FROM FACT_USER_EVENTS f
JOIN DIM_TRACK t
  ON f.track_id = t.track_id
JOIN DIM_DATE d
  ON f.event_date = d.date_key
GROUP BY
    d.year,
    d.month,
    t.track_id
ORDER BY
    d.year,
    d.month,
    revenue DESC;
