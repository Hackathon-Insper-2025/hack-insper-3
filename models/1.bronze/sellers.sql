SELECT 
    seller_zip_code_prefix as seller_zip_code_prefix,
    seller_id as seller_id,
    seller_city as seller_city,
    seller_state as seller_state,
    dex_ingestion_timestamp as dex_ingestion_timestamp
FROM 
    {{source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_sellers')}}