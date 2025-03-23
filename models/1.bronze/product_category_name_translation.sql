select
    cast (product_category_name as STRING) as product_category_name,
    cast (product_category_name_english as STRING) as product_category_name_english,
    cast (dex_ingestion_timestamp as TIMESTAMP) as dex_ingestion_timestamp
from
    {{source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_product_category_name_translation')}}

