{{
    config(
        materialized = 'table',
        unique_key = ['log_item_id', 'record_valid_from'],
        schema = 'core',
    )
}}

-- Deduplicate source data by ensuring a single row per log_item_id and record_valid_from
WITH deduplicated_logs AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY log_item_id, time_log_created_utc
            ORDER BY time_log_created_utc DESC
        ) AS row_number
    FROM {{ ref('staging_item_logs') }}
    WHERE log_item_id IS NOT NULL -- Ensure no NULL keys
),
filtered_logs AS (
    SELECT *
    FROM deduplicated_logs
    WHERE row_number = 1 -- Keep only the most relevant record
),

-- Rank items to handle Slowly Changing Dimension logic
ranked_items AS (
    SELECT
        item_key,
        log_item_id,
        time_log_created_utc AS record_valid_from,
        LEAD(time_log_created_utc) OVER (
            PARTITION BY item_key
            ORDER BY time_log_created_utc
        ) AS record_valid_to,
        brand_name,
        item_category,
        number_of_units,
        weight_in_grams,
        currency,
        product_base_price,
        product_base_price / (1 + vat_rate / 100) AS product_base_price_ex_vat,
        vat_rate,
        CASE
            WHEN name_lang_0 = 'en' THEN name_value_0
            WHEN name_lang_1 = 'en' THEN name_value_1
            ELSE NULL
        END AS product_name_en
    FROM filtered_logs
)
SELECT
    item_key,
    log_item_id,
    brand_name,
    item_category,
    number_of_units,
    weight_in_grams,
    currency,
    product_base_price,
    product_base_price_ex_vat,
    vat_rate,
    product_name_en,
    record_valid_from,
    record_valid_to
FROM ranked_items