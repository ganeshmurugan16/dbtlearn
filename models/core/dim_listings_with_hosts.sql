{{config(materialized='table')}}
with dim_listings_cleansed 
as
(
    select
            *
    from {{ref('dim_listings_cleansed')}}
),
dim_cleansed_host
as
(
    select  
            *
    from {{ref('dim_cleansed_host')}}
)

 select 
    l.listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    h.host_id,
    h.host_name,
    h.is_superhost as host_is_superhost,
    l.created_at as created_at,
    greatest(l.updated_at, h.updated_at) as updated_at    
from dim_listings_cleansed l
join dim_cleansed_host h on l.host_id = h.host_id
where l.listing_id is not null
  and h.host_id is not null