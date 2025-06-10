-- Simple Revenue Analysis
-- Monthly revenue summary

select 
    date_trunc('month', order_date) as month,
    count(*) as total_orders,
    sum(amount) as total_revenue,
    avg(amount) as average_order_value,
    count(distinct customer_id) as unique_customers
    
from "jaffle_shop"."main"."fct_orders"
where order_status = 'completed'
group by date_trunc('month', order_date)
order by month desc