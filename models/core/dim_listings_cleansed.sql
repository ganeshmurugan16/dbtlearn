{{config(materialized= 'view')}}

with src_listings
as
(
    select 
            *
    from {{ref('src_listing')}}
)

select 
         LISTING_ID
        ,LISTING_NAME
        ,LISTING_URL
        ,ROOM_TYPE
        ,case when MINIMUM_NIGHTS =1 then 0 else MINIMUM_NIGHTS end MINIMUM_NIGHTS
        ,HOST_ID
        ,replace(PRICE_STR,'$','')::number price 
        ,CREATED_AT
        ,UPDATED_AT
from src_listings