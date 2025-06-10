

with payments as (
    select
        id as payment_id,
        order_id,
        payment_method,
        amount / 100.0 as amount_usd,
        created_at,
        updated_at
    from "jaffle_shop"."main"."raw_payments"
    where status = 'success'
)

select * from payments