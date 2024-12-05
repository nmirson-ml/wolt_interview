SELECT * FROM {{ ref('fact_order_item') }}
WHERE 
    -- Negative item counts
    item_count < 0
    -- Very high item counts (assuming a reasonable upper limit, e.g., 1000)
    OR item_count > 1000
    -- Negative product prices
    OR product_price < 0
    -- Very high product prices
    OR product_price > 10000
    -- Product price ex VAT higher than product price
    OR product_price_ex_vat > product_price
    -- Negative weights
    OR weight_in_grams < 0
    -- Unreasonably high weights
    OR weight_in_grams > 10000
    -- Invalid currency codes (assuming valid codes are "EUR", "USD", etc.)
    OR currency NOT LIKE  '" EUR"'
    -- Invalid discount percentages
    OR discount < 0
    OR discount > 100