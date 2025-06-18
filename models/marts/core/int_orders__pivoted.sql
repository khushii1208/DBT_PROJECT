with payments as (
   select * from {{ ref('stg_payments') }}
),
 
final as (
   select
       order_id,
 
       sum(case when payment_method = 'bank_transfer' then amount_usd else 0 end) as bank_transfer_amount,
       sum(case when payment_method = 'credit_card' then amount_usd else 0 end) as credit_card_amount,
       sum(case when payment_method = 'coupon' then amount_usd else 0 end) as coupon_amount,
       sum(case when payment_method = 'gift_card' then amount_usd else 0 end) as gift_card_amount
       from payments
       group by order_id
)
 
select * from final