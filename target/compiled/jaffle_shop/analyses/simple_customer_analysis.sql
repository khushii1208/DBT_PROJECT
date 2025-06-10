-- Simple Customer Analysis
-- Shows customer behavior in easy-to-understand format

select 
    customer_id,
    first_name,
    last_name,
    number_of_orders,
    lifetime_value,
    
    -- Simple customer categories
    case 
        when number_of_orders = 1 then 'One-time Customer'
        when number_of_orders <= 3 then 'Regular Customer'
        else 'Loyal Customer'
    end as customer_type,
    
    -- Simple value categories
    case 
        when lifetime_value < 100 then 'Low Spender'
        when lifetime_value < 300 then 'Medium Spender'
        else 'High Spender'
    end as spending_level
    
from "jaffle_shop"."main"."dim_customers"
where number_of_orders > 0
order by lifetime_value desc
limit 20