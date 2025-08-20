{{  config( materialized='ephemeral' ) }}

--src_listings.sql
WITH src_listings AS (
    SELECT
        *
    FROM
         {{source('raw','listing')}}
)
SELECT
    id AS listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price AS price_str,
    created_at,
    updated_at
from src_listings