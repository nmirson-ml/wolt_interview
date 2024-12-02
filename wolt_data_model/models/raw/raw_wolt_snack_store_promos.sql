{{ config(schema='raw') }}
SELECT
    PROMO_START_DATE,
    PROMO_END_DATE,
    ITEM_KEY,
    PROMO_TYPE,
    DISCOUNT_IN_PERCENTAGE,
    CURRENT_TIMESTAMP AS loaded_timestamp
FROM {{ ref('Wolt_snack_store_promos') }}
