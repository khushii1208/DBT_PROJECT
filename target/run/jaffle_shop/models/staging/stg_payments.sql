
  create or replace   view DBT_TEST.dbt_kbhatia.stg_payments
  
  
  
  
  as (
    

select
    id as payment_id,
    order_id,
    payment_method,
    amount / 100.0 as amount_usd,
    created_at,
    updated_at
from DBT_TEST.dbt_kbhatia.raw_payments
where status = 'success'
  );

