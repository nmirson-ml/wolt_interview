{{ config(
    materialized = 'table',
    schema='staging'
) }}
WITH expanded_basket AS (
    SELECT
        -- Top-level fields
        REPLACE(TIME_ORDER_RECEIVED_UTC, 'Z', '')::TIMESTAMP AS time_order_received_utc,
        PURCHASE_KEY AS purchase_key,
        CUSTOMER_KEY AS customer_key,
        DELIVERY_DISTANCE_LINE_METERS::INTEGER AS delivery_distance_line_meters,
        WOLT_SERVICE_FEE::DECIMAL(10, 2) AS wolt_service_fee,
        COURIER_BASE_FEE::DECIMAL(10, 2) AS courier_base_fee,
        TOTAL_BASKET_VALUE::DECIMAL(10, 2) AS total_basket_value,

        -- Extract the entire basket as a structured array
        ITEM_BASKET_DESCRIPTION AS basket_items

    FROM {{ ref('raw_wolt_snack_store_purchase_logs') }}
)
SELECT
    -- Top-level fields
    time_order_received_utc,
    purchase_key,
    customer_key,
    delivery_distance_line_meters,
    wolt_service_fee,
    courier_base_fee,
    total_basket_value,
    basket_items,
    CURRENT_TIMESTAMP AS loaded_timestamp
FROM expanded_basket