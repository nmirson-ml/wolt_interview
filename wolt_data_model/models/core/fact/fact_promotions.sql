{{
    config(
        materialized = 'table',
        unique_key = ['purchase_key', 'item_key', 'promo_start_date', 'promo_end_date'],
        schema = 'core',
    )
}}
WITH unpacked_purchases AS (
    -- Unpivot the item keys and their counts into separate rows
     SELECT
        purchase_key,
        time_order_received_utc,
        CASE idx
            {% for i in range(1, 10) %}
            WHEN {{ i }} THEN item_key_{{ i }}
            {% endfor %}
        END AS item_key,
        CASE idx
            {% for i in range(1, 10) %}
            WHEN {{ i }} THEN item_count_{{ i }}
            {% endfor %}
        END AS quantity
    FROM {{ ref('fact_purchases') }}
    CROSS JOIN (
        SELECT GENERATE_SERIES(1, 9) AS idx
    ) nums
    WHERE CASE idx
        {% for i in range(1, 10) %}
        WHEN {{ i }} THEN item_key_{{ i }} IS NOT NULL
        {% endfor %}
    END
),
valid_promotions AS (
    SELECT
        up.purchase_key,
        up.item_key,
        up.time_order_received_utc,
        up.quantity,
        pr.promo_start_date,
        pr.promo_end_date,
        pr.promo_type,
        pr.discount_in_percentage
    FROM unpacked_purchases up
    LEFT JOIN {{ ref('dim_promotions') }} pr
        ON up.item_key = pr.item_key
        AND up.time_order_received_utc::date >= pr.promo_start_date
        AND up.time_order_received_utc::date < pr.promo_end_date
),
ranked_promotions AS (
    -- Deduplicate by taking the most recent applicable promotion, in case there are overlapping promotions
    SELECT
        purchase_key,
        item_key,
        time_order_received_utc,
        quantity,
        promo_start_date,
        promo_end_date,
        promo_type,
        discount_in_percentage,
        ROW_NUMBER() OVER (
            PARTITION BY purchase_key, item_key
            ORDER BY promo_start_date DESC
        ) AS promo_rank
    FROM valid_promotions
)
SELECT
    purchase_key,
    item_key,
    time_order_received_utc,
    quantity,
    promo_start_date,
    promo_end_date,
    promo_type,
    discount_in_percentage,
    CASE
        WHEN promo_start_date IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_promo_applied
FROM ranked_promotions
WHERE promo_rank = 1