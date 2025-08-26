{{config(materialized='table',
            transient=False,
             database='ECOMMERCE')}}

with tbl_sale as (
    select
        tc.customer_id,
        tc.name,
        tc.email,
        sum(tor.total_amount) as Total_purchase_amount,
        count(order_id) as No_of_item_order
    from  {{source('ECOMMERCE_raw','tbl_cust')}} tc
    inner join {{source('ECOMMERCE_raw','tbl_ord')}} tor
    on tc.customer_id  = tor.customer_id
    where is_current =true
    group by 
        tc.customer_id,
        tc.name,
        tc.email
)

select * from tbl_sale