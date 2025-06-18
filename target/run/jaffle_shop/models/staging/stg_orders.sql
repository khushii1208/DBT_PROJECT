
  create or replace   view DBT_TEST.dbt_kbhatia.stg_orders
  
  
  
  
  as (
    

select
    id as order_id,
    user_id as customer_id,
    order_date,
    status as order_status,
    created_at,
    updated_at
from DBT_TEST.dbt_kbhatia.raw_orders
  );

