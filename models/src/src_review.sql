{{  config( materialized='ephemeral' ) }}

with src_review 
as
(select 
        *
from {{source('raw','raw_reviews')}})

select 
        listing_id,
        date as review_date,
        reviewer_name,
        comments as review_text,
        sentiment as review_sentiment
 from src_review