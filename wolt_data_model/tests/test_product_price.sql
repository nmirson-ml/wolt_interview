select * from {{ref('dim_items_scd')}}
where product_base_price <= 0.00