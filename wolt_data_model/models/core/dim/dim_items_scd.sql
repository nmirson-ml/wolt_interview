{{
    config(
        materialized = 'incremental',
        schema = 'core',
        unique_key = ['log_item_id'],
        on_schema_change = 'fail'
    )
}}
WITH all_items AS (
    SELECT *
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
FROM all_items
{% if is_incremental() %}
    AND record_valid_from > (SELECT MAX(record_valid_from) FROM {{ this }})
{% endif %}