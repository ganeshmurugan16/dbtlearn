{{config (materialized='incremental',
            unique_key='customer_id',
            incremental_strategy='merge',
            transient=False,
            database='ECOMMERCE')}}

with dbt_staging_customer as 
(select  
        *
from {{source('ECOMMERCE_raw', 'tbl_customer')}})

select 
        raw:customer_id:: string AS customer_id,
        raw:name::string as Name ,
        raw:email::string as Email,
        raw:region::string AS region ,
        raw:signup_date:: datetime as signup_date
        
from dbt_staging_customer