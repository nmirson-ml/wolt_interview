{{ config(
    materialized = 'incremental',
    unique_key = ['purchase_key', 'customer_key'],
    schema = 'core',
    on_schema_change ='fail'
) }}


SELECT
    -- Purchase details
    purchase_key,
    time_order_received_utc,
    customer_key,
    delivery_distance_line_meters,
    wolt_service_fee,
    courier_base_fee,
    total_basket_value,
    
    -- Basket details
    {% for i in range(1, 10) %}
    item_count_{{ i }},
    item_key_{{ i }}{% if not loop.last %},{% endif %}
    {% endfor %},

    -- Promotion details (join with dim_promotions)
    promo_start_date,
    promo_end_date,
    promo_type,
    discount_in_percentage,

    CURRENT_TIMESTAMP AS loaded_timestamp
FROM {{ ref('staging_purchase_logs') }} sp
LEFT JOIN {{ ref('dim_promotions') }} dp
ON sp.item_key_1 = dp.item_key