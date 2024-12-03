{{
    config(
        materialized = 'table',
        unique_key = 'customer_id',
        schema = 'core'
)
}}
-- models/dim_customers.sql
SELECT
    customer_id,
    MIN(order_date) AS first_order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT order_id) > 1 AS is_repeat_customer,
    SUM(total_basket_value) AS total_spent
FROM {{ ref('fact_order') }}
GROUP BY customer_id
