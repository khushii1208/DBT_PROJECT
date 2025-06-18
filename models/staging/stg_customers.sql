{{ config(
    materialized='view',
    tags=['staging', 'customers']
) }}

select
    id as customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at
from {{ source('jaffle_shop', 'raw_customers') }}
