{{ config(
      materialized = 'table',
      unique_key = 'date',
      schema = 'core'
) }}
-- models/dim_time.sql
WITH calendar AS (
   SELECT CAST(RANGE AS DATE) AS calendar_date 
   FROM RANGE(DATE '2020-01-01', DATE '2024-12-31', INTERVAL 1 DAY)
)
SELECT
    CAST(calendar_date AS DATE) AS calendar_date,  -- Directly use calendar_date as a scalar DATE
    DATE_PART('year', calendar_date) AS year,
    DATE_PART('month', calendar_date) AS month,
    DATE_PART('day', calendar_date) AS day,
    DATE_PART('dow', calendar_date) AS day_of_week
FROM calendar