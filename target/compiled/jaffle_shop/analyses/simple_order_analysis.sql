-- Simple Order Analysis
-- Basic order patterns

select 
    order_status,
    count(*) as order_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    
    -- Percentage of total orders
    round(100.0 * count(*) / sum(count(*)) over(), 1) as pct_of_orders
    
from "jaffle_shop"."main"."fct_orders"
group by order_status
order by order_count desc