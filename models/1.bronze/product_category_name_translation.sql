select
    product_category_name,
    product_category_name_english,
    dex_ingestion_timestamp
from
    {{source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_product_category_name_translation')}}

