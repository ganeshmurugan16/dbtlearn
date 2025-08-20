{{config(materialized='table')}}

with host_list as
(
    select 
            *
    from {{source('raw','host_list')}}
)

select * from HOST_LIST where source='SQL Server'