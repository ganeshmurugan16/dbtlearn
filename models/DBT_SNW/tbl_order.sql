{{config(materialized='incremental',
            unique_key='order_id',
            incremental_strategy='merge',
            transient=False,
            database='ECOMMERCE')}}

with tbl_order as (
    select
            raw:customer_id:: number  as customer_id,
            raw:order_date ::datetime as order_date,
            raw:order_id:: number as order_id,
            raw:region::string as region,
            raw:tax_amount::float as tax_amount,
            raw:total_amount:: float as total_amount,
            raw:vat_amount:: number as vat_amount ,
            raw:workout_plan:: string as workout_plan
    from {{source('ECOMMERCE_raw', 'tbl_order')}}
)

select * from tbl_order