select * from {{ref('dim_items_scd')}}
where 
    -- Negative prices
    product_base_price < 0
    -- Very high prices 
    or product_base_price > 1000
    -- -- Price ex VAT higher than price with VAT
    or product_base_price_ex_vat >= product_base_price
    -- -- Zero prices or null prices (might be free items, gift with purchase or something similar)
    or product_base_price = 0
    or product_base_price IS NULL
    -- -- Test for invalid currency codes
    or currency NOT LIKE  '" EUR"'
    -- -- Negative weights
    or weight_in_grams <= 0
    -- -- Unreasonably high weights 
    or weight_in_grams > 10000
    -- -- Negative VA T
    or vat_rate < 0
    -- -- Unreasonably high VAT
    or vat_rate > 50