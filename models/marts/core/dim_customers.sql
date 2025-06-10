{{ config(
    materialized='table',
    tags=['core', 'daily', 'customers']
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from {{ ref('stg_orders') }}
    group by customer_id
),

customer_payments as (
    select
        orders.customer_id,
        sum(payments.amount_usd) as lifetime_value
    from {{ ref('stg_orders') }} orders
    left join {{ ref('stg_payments') }} payments
        on orders.order_id = payments.order_id
    group by orders.customer_id
),

final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.email,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_payments.lifetime_value, 0) as lifetime_value,
        customers.created_at,
        customers.updated_at
    from customers
    left join customer_orders on customers.customer_id = customer_orders.customer_id
    left join customer_payments on customers.customer_id = customer_payments.customer_id
)

select * from final