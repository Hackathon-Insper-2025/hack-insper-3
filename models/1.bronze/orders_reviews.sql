select 
    cast (review_score as INTEGER) as review_score,
    cast (review_id as STRING) as review_id,
    cast (order_id as STRING) as order_id,
    cast (review_comment_title as STRING) as review_comment_title,
    cast (review_comment_message as STRING) as review_comment_message,
    cast (review_creation_date as STRING) as review_creation_date,
    cast (review_answer_timestamp as STRING) as review_answer_timestamp,
    cast (dex_ingestion_timestamp as TIMESTAMP) as dex_ingestion_timestamp
from
    {{source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_order_reviews')}}