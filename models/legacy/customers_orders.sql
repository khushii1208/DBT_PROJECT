dbt-- Legacy customer_orders model (now using sources)
-- Demonstrates migration from hard-coded refs to dbt sources

select
    orders.id as order_id,
    orders.user_id as customer_id,
    customers.last_name as surname,
    customers.first_name as givenname,
    customer_order_history.first_order_date,
    customer_order_history.order_count,
    customer_order_history.total_lifetime_value,
    round(payments.amount/100.0,2) as order_value_dollars,
    orders.status as order_status

from {{ source('jaffle_shop', 'raw_orders') }} as orders

join (
    select
        first_name || ' ' || last_name as name,
        *
    from {{ source('jaffle_shop', 'raw_customers') }}
) customers
on orders.user_id = customers.id

join (
    select
        b.id as customer_id,
        b.first_name || ' ' || b.last_name as full_name,
        b.last_name as surname,
        b.first_name as givenname,
        min(a.order_date) as first_order_date,
        count(a.id) as order_count,
        sum(case when a.status NOT IN ('returned','return_pending') then c.amount/100.0 end) as total_lifetime_value

    from {{ source('jaffle_shop', 'raw_orders') }} a
    join {{ source('jaffle_shop', 'raw_customers') }} b
    on a.user_id = b.id

    left outer join {{ source('jaffle_shop', 'raw_payments') }} c
    on a.id = c.order_id

    where a.status NOT IN ('pending') and c.status = 'success'

    group by b.id, b.first_name, b.last_name
) customer_order_history
on orders.user_id = customer_order_history.customer_id

left outer join {{ source('jaffle_shop', 'raw_payments') }} payments
on orders.id = payments.order_id

where payments.status = 'success'