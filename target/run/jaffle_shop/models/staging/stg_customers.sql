
  create or replace   view DBT_TEST.dbt_kbhatia.stg_customers
  
  
  
  
  as (
    

select
    id as customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at
from DBT_TEST.dbt_kbhatia.raw_customers
  );

