{{
    config(
        materialized = 'incremental',
        schema = 'core',
        unique_key = ['log_item_id'],
        on_schema_change = 'fail'
    )
}}
WITH latest_items AS (
    SELECT *
    FROM {{ ref('staging_item_logs') }}
),
expanded_names AS (
    SELECT 
        log_item_id,
        item_key,
        brand_name,
        item_category,
        number_of_units,
        weight_in_grams,
        currency,
        product_base_price,
        product_base_price_ex_vat,
        vat_rate,
        record_valid_from,
        record_valid_to,
        TRIM('[]' FROM REPLACE(li.name_array, '\n', '')) as name_array_cleaned
    FROM latest_items li
),
split_items AS (
    SELECT 
        log_item_id,
        item_key,
        brand_name,
        item_category,
        number_of_units,
        weight_in_grams,
        currency,
        product_base_price,
        product_base_price_ex_vat,
        vat_rate,
        record_valid_from,
        record_valid_to,
        UNNEST(regexp_split_to_array(en.name_array_cleaned, '},  {')) AS item_list
    FROM expanded_names en
)
,extracted_items AS (
    SELECT 
        log_item_id,
        item_key,
        brand_name,
        item_category,
        number_of_units,
        weight_in_grams,
        currency,
        product_base_price,
        product_base_price_ex_vat,
        vat_rate,
        record_valid_from,
        record_valid_to,
        CAST(regexp_extract(si.item_list, '"lang":\s*(\d+)', 1) AS VARCHAR) AS lang,
        CAST(regexp_extract(si.item_list, '"value":\s*"([^"]+)"', 1) AS VARCHAR) AS product_name
    FROM split_items si
),
prioritized_names AS (
    SELECT 
        log_item_id,
        item_key,
        brand_name,
        item_category,
        FIRST_VALUE(product_name) OVER (
            PARTITION BY item_key
            ORDER BY CASE WHEN lang = 'en' THEN 0 ELSE 1 END
        ) AS product_name,
        number_of_units,
        weight_in_grams,
        currency,
        product_base_price,
        product_base_price_ex_vat,
        vat_rate,
        record_valid_from,
        record_valid_to
    FROM extracted_items
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
