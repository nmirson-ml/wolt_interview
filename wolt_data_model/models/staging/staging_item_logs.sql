{{
    config(
        materialized = 'table',
        schema = 'staging',
        unique_key = ['log_item_id']
    )
}}
WITH expanded_payload AS (
    SELECT
        -- Core identifiers
        LOG_ITEM_ID AS log_item_id,
        ITEM_KEY AS item_key,
        REPLACE(TIME_LOG_CREATED_UTC, 'Z', '')::TIMESTAMP AS time_log_created_utc,

        -- Extract fields from JSON payload
        JSON_EXTRACT(PAYLOAD, '$.brand_name')::VARCHAR AS brand_name,
        JSON_EXTRACT(PAYLOAD, '$.item_category')::VARCHAR AS item_category,

        -- Extract name array
        JSON_EXTRACT(payload, '$.name[*]') AS name_array,

        -- Rest of the fields...
        JSON_EXTRACT(PAYLOAD, '$.number_of_units')::INTEGER AS number_of_units,
        JSON_EXTRACT(PAYLOAD, '$.weight_in_grams')::INTEGER AS weight_in_grams,
        TRIM(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].currency'))::VARCHAR AS currency,
        NULLIF(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].product_base_price'), 'null')::DECIMAL(10, 4) AS product_base_price,
        NULLIF(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].vat_rate_in_percent'), 'null')::DECIMAL(10, 2) AS vat_rate,
        CASE
            WHEN JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].product_base_price') IS NOT NULL
                 AND JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].vat_rate_in_percent') IS NOT NULL
            THEN (
                NULLIF(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].product_base_price'), 'null')::DECIMAL(10, 4) /
                (1 + NULLIF(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].vat_rate_in_percent'), 'null')::DECIMAL(10, 2) / 100)
            )
            ELSE NULL
        END AS product_base_price_ex_vat,
        REPLACE(JSON_EXTRACT(PAYLOAD, '$.time_item_created_in_source_utc'), 'null', NULL)::TIMESTAMP AS time_item_created_in_source_utc
    FROM {{ ref('raw_wolt_snack_store_item_logs') }}
),
ranked_items AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_key
            ORDER BY time_log_created_utc
        ) AS row_number,
        LEAD(time_log_created_utc) OVER (
            PARTITION BY item_key
            ORDER BY time_log_created_utc
        ) AS record_valid_to
    FROM expanded_payload
)
SELECT
    log_item_id,
    item_key,
    brand_name,
    item_category,
    CAST(name_array AS VARCHAR) AS name_array,
    number_of_units,
    weight_in_grams,
    currency,
    product_base_price,
    product_base_price_ex_vat,
    vat_rate,
    time_log_created_utc AS record_valid_from,
    record_valid_to
FROM ranked_items
WHERE product_base_price IS NOT NULL
-- WHERE row_number = 1
--   OR NOT EXISTS (
--       SELECT 1
--       FROM {{ this }}
--       WHERE {{ this }}.log_item_id = ranked_items.log_item_id
--   )