{{ config(
    materialized='view',
    tags=['staging', 'payments']
) }}

select
    id as payment_id,
    order_id,
    payment_method,
     {{ cents_to_dollars('amount_usd', 4) }} as amount_usd,
    created_at,
    updated_at
from {{ source('jaffle_shop', 'raw_payments') }}
where status = 'success'