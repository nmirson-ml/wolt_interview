{{ config(
    materialized = 'table',
    schema='staging'
) }}

WITH expanded_payload AS (
  SELECT
        -- Top-level fields
        LOG_ITEM_ID,
        REPLACE(TIME_LOG_CREATED_UTC, 'Z', '')::TIMESTAMP AS time_log_created_utc,

        -- Extract individual fields from JSON
        JSON_EXTRACT(PAYLOAD, '$.brand_name')::VARCHAR AS brand_name,
        JSON_EXTRACT(PAYLOAD, '$.item_category')::VARCHAR AS item_category,
        JSON_EXTRACT(PAYLOAD, '$.item_key')::VARCHAR AS item_key,
        JSON_EXTRACT(PAYLOAD, '$.number_of_units')::INTEGER AS number_of_units,
        JSON_EXTRACT(PAYLOAD, '$.time_item_created_in_source_utc')::TIMESTAMP AS time_item_created_in_source_utc,
        JSON_EXTRACT(PAYLOAD, '$.weight_in_grams')::INTEGER AS weight_in_grams,

        -- Handle nullable fields
        JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].currency')::VARCHAR AS currency,
        -- Replace invalid values ("null") with NULL for decimals
        NULLIF(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].product_base_price'), 'null')::DECIMAL(10, 4) AS product_base_price,
        NULLIF(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].vat_rate_in_percent'), 'null')::DECIMAL(10, 2) AS vat_rate,

        -- Extract nested array: name translations
        JSON_EXTRACT(PAYLOAD, '$.name[0].lang')::VARCHAR AS name_lang_0,
        JSON_EXTRACT(PAYLOAD, '$.name[0].value')::VARCHAR AS name_value_0,
        JSON_EXTRACT(PAYLOAD, '$.name[1].lang')::VARCHAR AS name_lang_1,
        JSON_EXTRACT(PAYLOAD, '$.name[1].value')::VARCHAR AS name_value_1
    FROM {{ ref('raw_wolt_snack_store_item_logs') }}
)
SELECT
    -- Top-level fields
    LOG_ITEM_ID AS log_item_id,
    item_key,
    time_log_created_utc,
    brand_name,
    item_category,
    number_of_units,
    time_item_created_in_source_utc,
    weight_in_grams,

    -- Extracted name translations
    name_lang_0,
    name_value_0,
    name_lang_1,
    name_value_1,

    -- Extracted price attributes
    currency,
    product_base_price,
    vat_rate
FROM expanded_payload