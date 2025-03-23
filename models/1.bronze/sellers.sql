SELECT 
    cast (seller_zip_code_prefix as INTEGER) as seller_zip_code_prefix,
    cast (regexp_extract(seller_id,r'^.{0,2}') as STRING) as seller_id,
    cast (seller_city as STRING) as seller_city,
    cast (seller_state as STRING) as seller_state,
    cast (dex_ingestion_timestamp as TIMESTAMP) as dex_ingestion_timestamp
FROM 
    {{source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_sellers')}}