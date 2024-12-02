{{ config(schema='raw') }}
SELECT
    LOG_ITEM_ID,
    ITEM_KEY,
    TIME_LOG_CREATED_UTC,
    PAYLOAD,
    CURRENT_TIMESTAMP AS loaded_timestamp
FROM {{ ref('Wolt_snack_store_item_logs') }}
