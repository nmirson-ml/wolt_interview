{{
    config(
        materialized = 'incremental',
        schema = 'core',
        unique_key = ['log_item_id'],
        on_schema_change = 'fail'
    )
}}

SELECT 
   -- Top-level fields
    log_item_id,
    item_key,
    brand_name,
    item_category,
    product_name, -- Prioritized English name
    number_of_units,
    weight_in_grams,
    -- Extracted price attributes
    currency,
    product_base_price,
    product_base_price_ex_vat,
    vat_rate,
    -- incremental load fields
    record_valid_from,
    record_valid_to,
FROM {{ ref('staging_item_logs') }}
