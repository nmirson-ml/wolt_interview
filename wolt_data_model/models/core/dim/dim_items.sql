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
),
unnested_names AS (
    SELECT 
        li.*,
        name.value AS name_entry
    FROM latest_items li,
         UNNEST(li.name_array) AS name(value)
),
extracted_fields AS (
    SELECT 
        unnested_names.*,
        JSON_EXTRACT_STRING(name_entry, '$.lang') AS lang,
        JSON_EXTRACT_STRING(name_entry, '$.value') AS product_name
    FROM unnested_names
),
prioritized_names AS (
    SELECT 
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
        record_valid_from
    FROM extracted_fields
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
FROM prioritized_names