
  
  create view "jaffle_shop"."main"."stg_customers__dbt_tmp" as (
    

with customers as (
    select
        id as customer_id,
        first_name,
        last_name,
        email,
        created_at,
        updated_at
    from "jaffle_shop"."main"."raw_customers"
)

select * from customers
  );
