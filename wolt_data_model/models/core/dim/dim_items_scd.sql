{{
    config(
        materialized = 'incremental',
        schema = 'core',
        unique_key = ['log_item_id'],
        on_schema_change = 'fail'
    )
}}


WITH prioritized_names AS (
    SELECT 
        log_item_id,
        item_key,
        brand_name,
        item_category,
        FIRST_VALUE(product_name) OVER (
            PARTITION BY log_item_id
            ORDER BY 
                CASE WHEN lang = 'en' THEN 0 ELSE 1 END,
                lang  -- secondary sort by language code if no English
        ) AS product_name,
        number_of_units,
        weight_in_grams,
        currency,
        product_base_price,
        product_base_price_ex_vat,
        vat_rate,
        record_valid_from,
        record_valid_to
    FROM {{ ref('staging_item_logs') }}
)
SELECT DISTINCT
    log_item_id,
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
    record_valid_from,
    record_valid_to
FROM prioritized_names
