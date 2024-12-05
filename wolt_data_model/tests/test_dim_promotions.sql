SELECT * FROM {{ ref('dim_promotions') }}
WHERE
    -- Check for invalid date ranges
    promo_end_date < promo_start_date
    -- Check for missing promo types
    OR promo_type IS NULL
    -- Check for invalid discount percentages
    OR discount_in_percentage < 0
    OR discount_in_percentage > 100
    -- Check for missing item keys
    OR item_key IS NULL
    -- Check for promo_id NULL
    OR promo_id IS NULL