{{config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='append',
    transient=False,
    database='ECOMMERCE',    
    post_hook="delete from ECOMMERCE.raw.raw_order"
)}}

with source as (
    select
        raw:customer_id::number  as customer_id,
        raw:order_date::datetime as order_date,
        raw:order_id::number as order_id,
        raw:region::string as region,
        raw:tax_amount::float as tax_amount,
        raw:total_amount::float as total_amount,
        raw:vat_amount::number as vat_amount,
        raw:workout_plan::string as workout_plan
    from {{source('ECOMMERCE_raw', 'tbl_order')}}
),
existing_table as (
    select *
    from {{this}}
    where is_active = true
),
insert_record as (
    select
        s.customer_id,
        s.order_date,
        s.order_id,
        s.region,
        s.tax_amount,
        s.total_amount,
        s.vat_amount,
        s.workout_plan,
        current_timestamp() as created_at,
        null as modified_at,
        true as is_active
    from source s
    left join existing_table t
        on s.order_id = t.order_id
    where t.order_id is null
       or s.region != t.region
       or s.tax_amount != t.tax_amount
       or s.total_amount != t.total_amount
       or s.vat_amount != t.vat_amount
       or s.workout_plan != t.workout_plan
       or s.order_date != t.order_date
),
expire_record as (
    select
        t.customer_id,
        t.order_date,
        t.order_id,
        t.region,
        t.tax_amount,
        t.total_amount,
        t.vat_amount,
        t.workout_plan,
        t.created_at,
        current_timestamp() as modified_at,
        false as is_active
    from existing_table t
    join insert_record upd
        on t.order_id = upd.order_id
)

select * from insert_record
{% if is_incremental %}
union 
select * from expire_record
{% endif %}
