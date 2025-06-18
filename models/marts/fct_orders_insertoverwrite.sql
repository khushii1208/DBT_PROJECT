{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    unique_key = 'order_id',
    cluster_by = ['order_date']
) }}

with 

-- Import source data
base_customers as (
    select * from DBT_TEST.dbt_kbhatia.raw_customers
),

base_orders as (
    select * from DBT_TEST.dbt_kbhatia.raw_orders
),

base_payments as (
    select * from DBT_TEST.dbt_kbhatia.raw_payments
),

-- Simple business logic
customers as (
    select
        id as customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        email,
        created_at
    from base_customers
),

orders as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status as order_status,
        created_at
    from base_orders
    where status not in ('pending')

    {% if is_incremental() %}
      and order_date >= dateadd('day', -1, current_date)
    {% endif %}
),

payments as (
    select
        id as payment_id,
        order_id,
        payment_method,
        status as payment_status,
        amount / 100.0 as amount_usd,
        created_at
    from base_payments
    where status = 'success'
),

customer_order_metrics as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        min(o.order_date) as first_order_date,
        max(o.order_date) as most_recent_order_date,
        count(o.order_id) as total_order_count,
        sum(p.amount_usd) as lifetime_value
    from customers c
    left join orders o on c.customer_id = o.customer_id
    left join payments p on o.order_id = p.order_id
    group by c.customer_id, c.first_name, c.last_name, c.full_name
),

-- Final output
final as (
    select
        o.order_id,
        o.customer_id,
        cm.last_name as surname,
        cm.first_name as givenname,
        cm.first_order_date,
        cm.total_order_count as order_count,
        cm.lifetime_value as total_lifetime_value,
        p.amount_usd as order_value_dollars,
        o.order_status,
        o.order_date,
        o.created_at,
        current_timestamp() as dbt_loaded_at
    from orders o
    left join customer_order_metrics cm on o.customer_id = cm.customer_id
    left join payments p on o.order_id = p.order_id
)

select * from final
