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
expanded_names AS (
    SELECT 
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
        TRIM('[]' FROM REPLACE(li.name_array, '\n', '')) as name_array_cleaned
    FROM latest_items li
),
split_items AS (
    SELECT 
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
        UNNEST(regexp_split_to_array(en.name_array_cleaned, '},  {')) AS item_list
    FROM expanded_names en
)
,extracted_items AS (
    SELECT 
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
        CAST(regexp_extract(si.item_list, '"lang":\s*"([^"]+)"', 1) AS VARCHAR) AS lang,
        CAST(regexp_extract(si.item_list, '"value":\s*"([^"]+)"', 1) AS VARCHAR) AS product_name
    FROM split_items si
),
prioritized_names AS (
    SELECT 
        item_key,
        brand_name,
        item_category,
        FIRST_VALUE(product_name) OVER (
            PARTITION BY item_key
            ORDER BY 
                CASE 
                    WHEN lang = 'en' THEN 0 
                    WHEN lang IS NOT NULL THEN 1 
                    ELSE 2 
                END
        ) AS product_name,
        number_of_units,
        weight_in_grams,
        currency,
        product_base_price,
        product_base_price_ex_vat,
        vat_rate,
        record_valid_from
    FROM extracted_items
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