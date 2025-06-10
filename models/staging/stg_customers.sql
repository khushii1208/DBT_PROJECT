{{ config(
    materialized='view',
    tags=['staging', 'customers']
) }}

with customers as (
    select
        id as customer_id,
        first_name,
        last_name,
        email,
        created_at,
        updated_at
    from {{ ref('raw_customers') }}
)

select * from customers