{{  config( materialized='ephemeral' ) }}

with raw_hosts
as
    (select 
            *
    from {{source('raw','host')}}
    )

select 
        
            ID as host_id
            ,NAME as host_name
            ,IS_SUPERHOST
            ,CREATED_AT
            ,UPDATED_AT
 from raw_hosts where id is not null