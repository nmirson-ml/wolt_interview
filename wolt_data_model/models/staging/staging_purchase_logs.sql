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

        -- Dynamically extract item counts and keys from the basket
        {% for i in range(1, 10) %}
        JSON_EXTRACT(ITEM_BASKET_DESCRIPTION, '$[{{ i }}].item_count')::INTEGER AS item_count_{{ i }},
        JSON_EXTRACT(ITEM_BASKET_DESCRIPTION, '$[{{ i }}].item_key')::VARCHAR AS item_key_{{ i }}{% if not loop.last %},{% endif %}
        {% endfor %}
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

    -- Dynamically extracted basket details
    {% for i in range(1, 10) %}
    item_count_{{ i }},
    item_key_{{ i }}{% if not loop.last %},{% endif %}
    {% endfor %},

    CURRENT_TIMESTAMP AS loaded_timestamp
FROM expanded_basket