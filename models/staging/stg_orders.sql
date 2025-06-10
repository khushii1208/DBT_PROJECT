{{ config(
    materialized='view',
    tags=['staging', 'orders']
) }}

with orders as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status as order_status,
        created_at,
        updated_at
    from {{ ref('raw_orders') }}
)

select * from orders