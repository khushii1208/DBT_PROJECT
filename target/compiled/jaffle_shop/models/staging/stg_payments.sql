

select
    id as payment_id,
    order_id,
    payment_method,
     round( 1.0 * amount_usd / 100, 4) as amount_usd,
    created_at,
    updated_at
from DBT_TEST.dbt_kbhatia.raw_payments
where status = 'success'