{{
    config(
        materialized = 'incremental',
        schema = 'staging',
        unique_key = ['log_item_id'],
        on_schema_change = 'fail'
    )
}}

WITH expanded_payload AS (
    SELECT
        LOG_ITEM_ID AS log_item_id,
        ITEM_KEY AS item_key,
        REPLACE(TIME_LOG_CREATED_UTC, 'Z', '')::TIMESTAMP AS time_log_created_utc,
        JSON_EXTRACT(PAYLOAD, '$.brand_name')::VARCHAR AS brand_name,
        JSON_EXTRACT(PAYLOAD, '$.item_category')::VARCHAR AS item_category,
        JSON_EXTRACT(payload, '$.name[*]') AS name_array,
        JSON_EXTRACT(PAYLOAD, '$.number_of_units')::INTEGER AS number_of_units,
        JSON_EXTRACT(PAYLOAD, '$.weight_in_grams')::INTEGER AS weight_in_grams,
        TRIM(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].currency'))::VARCHAR AS currency,
        ABS(NULLIF(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].product_base_price'), 'null')::DECIMAL(10, 4)) AS product_base_price,
        NULLIF(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].vat_rate_in_percent'), 'null')::DECIMAL(10, 2) AS vat_rate,
        CASE
            WHEN JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].product_base_price') IS NOT NULL
                 AND JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].vat_rate_in_percent') IS NOT NULL
            THEN (
                ABS(NULLIF(JSON_EXTRACT(PAYLOAD, '$.price_attributes[0].product_base_price'), 'null')::DECIMAL(10, 4)) /
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
),
names_opened AS (
    SELECT
        *,
        json_extract(name_array, '$[0]') AS name_json_1,
        json_extract(name_array, '$[1]') AS name_json_2
    FROM ranked_items
),
opened_names AS (
    SELECT 
        *,
        json_extract_string(name_json_1, '$.lang') AS lang,
        json_extract_string(name_json_1, '$.value') AS name1,
        json_extract_string(name_json_2, '$.lang') AS lang2,
        json_extract_string(name_json_2, '$.value') AS name2
    FROM names_opened
)
SELECT
    log_item_id,
    item_key,
    brand_name,
    item_category,
    CASE 
        WHEN lang = 'en' THEN name1 
        WHEN lang2 = 'en' THEN name2
        ELSE name1  -- fallback to first name if no English version exists
    END AS product_name,
    number_of_units,
    weight_in_grams,
    currency,
    COALESCE(product_base_price,LAG(product_base_price) OVER (PARTITION BY item_key ORDER BY time_log_created_utc)) AS product_base_price,
    COALESCE(product_base_price_ex_vat,LAG(product_base_price_ex_vat) OVER (PARTITION BY item_key ORDER BY time_log_created_utc)) AS product_base_price_ex_vat,
    vat_rate,
    time_log_created_utc AS record_valid_from,
    record_valid_to
FROM opened_names
{% if is_incremental() %}
    AND record_valid_from > (SELECT MAX(record_valid_from) FROM {{ this }})
{% endif %}