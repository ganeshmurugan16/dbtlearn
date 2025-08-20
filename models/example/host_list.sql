{{ config(
    materialized='incremental',
    pre_hook="delete from {{ this }} where source = 'test'"
) }}

with host_list as (
    select 
        'oracle' as source,
        h.name as Hostname,
        h.is_superhost,
        rl.name as Listing_name,
        rl.room_type,
        rl.price,
        rl.listing_url
    from {{source('raw','host')}} h
    inner join {{source('raw','listing')}}  rl
        on h.id = rl.host_id
)

select * from host_list
