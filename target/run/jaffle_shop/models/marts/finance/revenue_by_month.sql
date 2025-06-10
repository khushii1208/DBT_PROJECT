
  
    
    

    create  table
      "jaffle_shop"."main"."revenue_by_month__dbt_tmp"
  
    as (
      

with monthly_revenue as (
    select
        date_trunc('month', order_date) as order_month,
        sum(amount) as monthly_revenue,
        count(distinct order_id) as monthly_orders,
        count(distinct customer_id) as monthly_customers,
        avg(amount) as avg_order_value
    from "jaffle_shop"."main"."fct_orders"
    where order_status not in ('returned', 'cancelled')
    group by date_trunc('month', order_date)
)

select 
    order_month,
    monthly_revenue,
    monthly_orders,
    monthly_customers,
    avg_order_value,
    lag(monthly_revenue) over (order by order_month) as prev_month_revenue,
    (monthly_revenue - lag(monthly_revenue) over (order by order_month)) / 
        lag(monthly_revenue) over (order by order_month) * 100 as revenue_growth_pct
from monthly_revenue
order by order_month
    );
  
  