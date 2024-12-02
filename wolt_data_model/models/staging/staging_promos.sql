{{ config(
    materialized = 'table',
    schema='staging'
) }}

SELECT
    PROMO_START_DATE::DATE AS promo_start_date,
    PROMO_END_DATE::DATE AS promo_end_date,
    ITEM_KEY AS item_key,
    PROMO_TYPE::VARCHAR AS promo_type,
    DISCOUNT_IN_PERCENTAGE::INTEGER AS discount_in_percentage,
    CURRENT_TIMESTAMP AS loaded_timestamp
FROM {{ ref('raw_wolt_snack_store_promos') }}
