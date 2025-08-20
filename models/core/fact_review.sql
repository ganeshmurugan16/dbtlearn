{{config(materialized='incremental',
  on_schema_change = 'fail'
)}}

with  src_review
as
(
    select  
            *
    from {{ref('src_review')}}
)

select 
        *
from  src_review 
where 
      review_text is not null
      {% if is_incremental()%}
      and review_date> (select max(review_date) from {{this}})
      {% endif %}