SELECT * FROM {{ref('fact_order')}}
WHERE 
    -- Negative total basket value
    total_basket_value < 0
    -- Negative wolt service fee
    OR wolt_service_fee < 0
    -- Negative courier base fee
    OR courier_base_fee < 0
    -- Unreasonably distance
    OR delivery_distance_line_meters < 0
    OR delivery_distance_line_meters > 250000
    -- Loaded timestamp is in the future
    OR loaded_timestamp > current_timestamp
