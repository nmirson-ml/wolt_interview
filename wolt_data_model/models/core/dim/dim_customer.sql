{{
    config(
        materialized = 'table',
        unique_key = 'customer_key',
        schema = 'core'
)
}}
-- models/dim_customers.sql
SELECT
    customer_key,
    CAST(MIN(time_order_received_utc) AS DATE) AS first_purchase_date,
    COUNT(DISTINCT purchase_key) AS total_purchases,
    COUNT(DISTINCT purchase_key) > 1 AS is_repeat_customer
FROM {{ ref('staging_purchase_logs') }}
GROUP BY customer_key
