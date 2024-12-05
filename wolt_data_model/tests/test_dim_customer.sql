
SELECT
    customer_id
FROM {{ ref('dim_customer') }}
WHERE 
-- Test for missing first order date
first_order_date IS NULL
-- Test for repeat customer logic
OR (is_repeat_customer = 1 AND total_orders <= 1)
OR (is_repeat_customer = 0 AND total_orders > 1)
-- Test for Negative Spend
OR total_spent < 0