select * from {{ref('fact_order')}}
where total_basket_value <= 0.00