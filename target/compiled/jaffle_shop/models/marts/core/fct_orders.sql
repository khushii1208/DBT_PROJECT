{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    unique_key = 'order_id',
    on_schema_change = 'fail'
) }}

with 

-- Import CTEs (source data)
base_customers as (
    select * from DBT_TEST.dbt_kbhatia.raw_customers
),

base_orders as (
    select * from DBT_TEST.dbt_kbhatia.raw_orders
),

base_payments as (
    select * from DBT_TEST.dbt_kbhatia.raw_payments
),

-- Data cleaning with NULL handling
customers as (
    select
        id as customer_id,
        coalesce(first_name, 'Unknown') as first_name,
        coalesce(last_name, 'Customer') as last_name,
        coalesce(first_name, 'Unknown') || ' ' || coalesce(last_name, 'Customer') as full_name,
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
    
    -- APPEND INCREMENTAL LOGIC
    {% if is_incremental() %}
      and created_at > (select coalesce(max(created_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

-- Aggregate payments by order_id (simplified - no STRING_AGG)
payments_aggregated as (
    select
        order_id,
        sum(amount / 100.0) as total_amount_usd,
        count(*) as payment_count,
        min(created_at) as first_payment_at,
        max(created_at) as last_payment_at
    from base_payments
    where status = 'success'
    group by order_id
),

-- Calculate customer metrics with better NULL handling
customer_order_metrics as (
    select
        coalesce(c.customer_id, o.customer_id) as customer_id,
        coalesce(c.first_name, 'Unknown') as first_name,
        coalesce(c.last_name, 'Customer') as last_name,
        coalesce(c.full_name, 'Unknown Customer') as full_name,
        min(o.order_date) as first_order_date,
        max(o.order_date) as most_recent_order_date,
        count(distinct o.order_id) as total_order_count,
        coalesce(sum(p.total_amount_usd), 0) as lifetime_value
    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join payments_aggregated p on o.order_id = p.order_id
    group by 
        coalesce(c.customer_id, o.customer_id),
        coalesce(c.first_name, 'Unknown'),
        coalesce(c.last_name, 'Customer'),
        coalesce(c.full_name, 'Unknown Customer')
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
        coalesce(p.total_amount_usd, 0) as order_value_dollars,
        o.order_status,
        o.order_date,
        o.created_at,
        current_timestamp() as dbt_loaded_at
        
    from orders o
    left join customer_order_metrics cm on o.customer_id = cm.customer_id
    left join payments_aggregated p on o.order_id = p.order_id
)

select * from final