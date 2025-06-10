
  
    
    

    create  table
      "jaffle_shop"."main"."fct_orders__dbt_tmp"
  
    as (
      

with orders as (
    select * from "jaffle_shop"."main"."stg_orders"
),

payments as (
    select
        order_id,
        sum(amount_usd) as total_amount
    from "jaffle_shop"."main"."stg_payments"
    group by order_id
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.order_status,
        coalesce(payments.total_amount, 0) as amount,
        orders.created_at,
        orders.updated_at
    from orders
    left join payments on orders.order_id = payments.order_id
)

select * from final
    );
  
  