{{
    config(
        materialized = 'incremental',
        unique_key = 'order_item_id',
        schema = 'core',
        on_schema_change = 'fail'
    )
}}

WITH parsed_basket_items AS (
    SELECT
        pl.customer_key AS customer_id,
        pl.purchase_key AS order_id,
        pl.time_order_received_utc AS order_timestamp,
        CAST(pl.time_order_received_utc AS DATE) AS order_date,
        basket_items,
        json_array_length(basket_items) AS array_size
    FROM {{ref ('staging_purchase_logs')}} pl
),
split_basket AS (
    SELECT 
  customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
  array_size,
        json_extract(basket_items, '$[0]') AS json_1,
        json_extract(basket_items, '$[1]') AS json_2,
        json_extract(basket_items, '$[2]') AS json_3,
        json_extract(basket_items, '$[3]') AS json_4,
        json_extract(basket_items, '$[4]') AS json_5,
        json_extract(basket_items, '$[5]') AS json_6,
        json_extract(basket_items, '$[6]') AS json_7,
        json_extract(basket_items, '$[7]') AS json_8,
        json_extract(basket_items, '$[8]') AS json_9
    FROM parsed_basket_items
),
  extracted_items AS( 
  SELECT 
  customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
  REPLACE(CAST(json_extract(json_1, '$.item_key') AS VARCHAR), '"', '') AS item_key,
  CAST(json_extract(json_1, '$.item_count') AS INTEGER) AS item_count
FROM split_basket
WHERE json_1 IS NOT NULL
UNION ALL
SELECT 
     customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
    REPLACE(CAST(json_extract(json_2, '$.item_key') AS VARCHAR), '"', '') AS item_key,
    CAST(json_extract(json_2, '$.item_count') AS INTEGER) AS item_count
FROM split_basket
WHERE json_2 IS NOT NULL
UNION ALL
SELECT 
     customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
    REPLACE(CAST(json_extract(json_3, '$.item_key') AS VARCHAR), '"', '') AS item_key,
    CAST(json_extract(json_3, '$.item_count') AS INTEGER) AS item_count
FROM split_basket
WHERE json_3 IS NOT NULL
UNION ALL
SELECT 
     customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
    REPLACE(CAST(json_extract(json_4, '$.item_key') AS VARCHAR), '"', '') AS item_key,
    CAST(json_extract(json_4, '$.item_count') AS INTEGER) AS item_count
FROM split_basket
WHERE json_4 IS NOT NULL
UNION ALL
SELECT 
     customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
    REPLACE(CAST(json_extract(json_5, '$.item_key') AS VARCHAR), '"', '') AS item_key,
    CAST(json_extract(json_5, '$.item_count') AS INTEGER) AS item_count
FROM split_basket
WHERE json_5 IS NOT NULL
UNION ALL
SELECT 
     customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
    REPLACE(CAST(json_extract(json_6, '$.item_key') AS VARCHAR), '"', '') AS item_key,
    CAST(json_extract(json_6, '$.item_count') AS INTEGER) AS item_count
FROM split_basket
WHERE json_6 IS NOT NULL
UNION ALL
SELECT 
     customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
    REPLACE(CAST(json_extract(json_7, '$.item_key') AS VARCHAR), '"', '') AS item_key,
    CAST(json_extract(json_7, '$.item_count') AS INTEGER) AS item_count
FROM split_basket
WHERE json_7 IS NOT NULL
UNION ALL
SELECT 
     customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
    REPLACE(CAST(json_extract(json_8, '$.item_key') AS VARCHAR), '"', '') AS item_key,
    CAST(json_extract(json_8, '$.item_count') AS INTEGER) AS item_count
FROM split_basket
WHERE json_8 IS NOT NULL
UNION ALL
SELECT 
     customer_id,
  order_id,
  basket_items,
  order_timestamp,
  order_date,
    REPLACE(CAST(json_extract(json_9, '$.item_key') AS VARCHAR), '"', '') AS item_key,
    CAST(json_extract(json_9, '$.item_count') AS INTEGER) AS item_count
FROM split_basket
WHERE json_9 IS NOT NULL
)
,valid_items AS (
    SELECT
        ei.customer_id,
        ei.order_id,
        ei.basket_items,
        ei.order_timestamp,
        ei.order_date,
        li.item_key,
        li.product_name,
        li.currency,
        li.brand_name,
        li.item_category,
        li.product_base_price AS product_price,
        li.product_base_price_ex_vat AS product_price_ex_vat,
        li.weight_in_grams
    FROM {{ref ('dim_items_scd')}} li
   JOIN extracted_items ei ON CAST(li.item_key AS VARCHAR) = CAST(ei.item_key AS VARCHAR)
    WHERE (ei.order_timestamp BETWEEN li.record_valid_from AND li.record_valid_to)
      OR  (ei.order_timestamp >= li.record_valid_from AND li.record_valid_to IS NULL)
      )
,
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
        vi.customer_id,
        vi.order_id,
        vi.item_key,
        vi.order_date,
        vi.order_timestamp,
        vi.product_name,
        vi.currency,
        vi.product_price,
        vi.product_price_ex_vat,
        vi.weight_in_grams,
        vi.brand_name,
        vi.item_category,
        CASE
            WHEN vi.order_timestamp BETWEEN p.promo_start_date AND p.promo_end_date THEN TRUE
            ELSE FALSE
        END AS had_promotion,
        CASE
            WHEN vi.order_timestamp BETWEEN p.promo_start_date AND p.promo_end_date THEN p.discount_in_percentage
            ELSE 0
        END AS discount
    FROM valid_items vi
    LEFT JOIN promotions p ON CAST(vi.item_key AS VARCHAR) = CAST(p.item_key AS VARCHAR)
)

SELECT DISTINCT
    customer_id,
    order_id,
    md5(CONCAT(order_id, item_key, order_timestamp)) AS order_item_id,
    CAST(item_key AS VARCHAR) AS item_id,
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