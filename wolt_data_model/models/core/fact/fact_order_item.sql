{{
    config(
        materialized = 'incremental',
        unique_key = 'order_item_id',
        schema = 'core',
        on_schema_change = 'fail'
    )
}}

WITH expanded_orders AS (
    SELECT
        pl.customer_key AS customer_id,
        pl.purchase_key AS order_id,
        pl.time_order_received_utc AS order_timestamp,
        CAST(pl.time_order_received_utc AS DATE) AS order_date,
        TRIM('[]' FROM REPLACE(pl.basket_items, '\n', '')) AS basket_items_cleaned
    FROM {{ ref('staging_purchase_logs') }} pl
),
split_items AS (
SELECT 
eo.customer_id, 
eo.order_id,
eo.order_timestamp,
eo.order_date,
UNNEST(regexp_split_to_array(eo.basket_items_cleaned, '},  {')) AS item_list
FROM expanded_orders eo
)
,
extracted_items AS (
    SELECT
        si.customer_id,
        si.order_id, 
        si.order_timestamp,
        si.order_date,
        CAST(regexp_extract(si.item_list, '"item_count":\s*(\d+)', 1) AS INTEGER) AS item_count,
        CAST(regexp_extract(si.item_list, '"item_key":\s*"([^"]+)"', 1) AS VARCHAR) AS item_key
    FROM split_items si)

,valid_items AS (
    SELECT
        -- li.log_item_id AS item_id,
        li.item_key,
        li.product_name,
        li.currency,
        li.brand_name,
        li.item_category,
        li.product_base_price AS product_price,
        li.product_base_price_ex_vat AS product_price_ex_vat,
        li.weight_in_grams
    FROM {{ ref('dim_items_scd') }} li
    WHERE li.record_valid_from <= eo.order_timestamp
      AND li.record_valid_to > eo.order_timestamp
),

promotions AS (
    SELECT
        dp.item_key,
        dp.promo_start_date,
        dp.promo_end_date,
        dp.discount_in_percentage
    FROM {{ ref('dim_promotions') }} dp
),

order_items AS (
    SELECT
        eo.customer_id,
        eo.order_id,
        eo.item_key,
        eo.order_date,
        eo.order_timestamp,
        vi.product_name,
        vi.currency,
        vi.product_price,
        vi.product_price_ex_vat,
        vi.weight_in_grams,
        vi.brand_name,
        vi.item_category,
        CASE
            WHEN eo.order_timestamp BETWEEN p.promo_start_date AND p.promo_end_date THEN TRUE
            ELSE FALSE
        END AS had_promotion,
        CASE
            WHEN eo.order_timestamp BETWEEN p.promo_start_date AND p.promo_end_date THEN p.discount_in_percentage
            ELSE 0
        END AS discount
    FROM extracted_items eo
    JOIN valid_items vi ON eo.item_key = vi.item_key
    LEFT JOIN promotions p ON eo.item_key = p.item_key
)

SELECT DISTINCT
    customer_id,
    order_id,
    md5(CONCAT(order_id, item_key, order_timestamp)) AS order_item_id,
    item_key AS item_id,
    order_date,
    order_timestamp,
    product_name,
    brand_name,
    item_category,
    currency,
    product_price,
    product_price_ex_vat,
    weight_in_grams,
    had_promotion,
    discount,
    CURRENT_TIMESTAMP AS loaded_timestamp
FROM order_items
{% if is_incremental() %}
WHERE order_timestamp > (SELECT MAX(order_timestamp) FROM {{ this }})
{% endif %}