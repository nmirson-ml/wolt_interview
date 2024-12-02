{{
config(
materialized = 'table',
unique_key = 'promo_id',
schema = 'core'
)
}}

SELECT
    md5(CONCAT(promo_start_date, promo_type, item_key)) AS promo_id,
    promo_start_date,
    promo_end_date,
    item_key,
    promo_type,
    discount_in_percentage,
    CURRENT_TIMESTAMP AS loaded_timestamp
FROM {{ ref('staging_promos') }}
