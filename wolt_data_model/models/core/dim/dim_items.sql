{{
    config(
        materialized = 'table',
        schema = 'core',
        unique_key = ['item_key']
    )
}}


WITH latest_items AS (
    -- Filter for the latest records where record_valid_to is NULL
    SELECT *
    FROM {{ ref('staging_item_logs') }}
    WHERE record_valid_to IS NULL
)
SELECT DISTINCT
    item_key,
    brand_name,
    item_category,
    product_name,
    number_of_units,
    weight_in_grams,
    currency,
    product_base_price,
    product_base_price_ex_vat,
    vat_rate,
    record_valid_from
FROM latest_items