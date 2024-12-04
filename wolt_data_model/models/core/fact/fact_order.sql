{{
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        schema = 'core',
        on_schema_change = 'fail'
    )
}}

SELECT 
    purchase_key AS order_id,
    customer_key AS customer_id,
    CAST(time_order_received_utc AS DATE) AS order_date,
    time_order_received_utc AS order_timestamp,
    delivery_distance_line_meters,
    wolt_service_fee,
    courier_base_fee,
    total_basket_value,
    CURRENT_TIMESTAMP AS loaded_timestamp
FROM {{ ref('staging_purchase_logs') }}
{% if is_incremental() %}
WHERE time_order_received_utc > (SELECT MAX(order_timestamp) FROM {{ this }})
{% endif %}