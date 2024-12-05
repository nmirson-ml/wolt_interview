select * from {{ref('dim_items')}}
where 
    -- Negative prices
    product_base_price < 0
    -- Very high prices (adjust threshold as needed)
    or product_base_price > 1000
    -- -- Price ex VAT higher than price with VAT
    or product_base_price_ex_vat >= product_base_price
    -- -- Zero prices (might be valid, but worth checking)
    or product_base_price = 0
    -- -- Test for invalid currency codes
    or currency NOT LIKE  '" EUR"'
    -- -- Negative weights
    or weight_in_grams <= 0
    -- -- Unreasonably high weights (adjust threshold as needed)
    or weight_in_grams > 10000
    -- -- Negative VA T
    or vat_rate < 0
    -- -- Unreasonably high VAT
    or vat_rate > 50