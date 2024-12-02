{{
    config(
        materialized = 'incremental',
        schema = 'core',
        unique_key = ['log_item_id'],
        on_schema_change = 'fail'
    )
}}

SELECT * FROM {{ ref('staging_item_logs') }}
