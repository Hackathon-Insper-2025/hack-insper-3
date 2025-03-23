SELECT 
    cast (customer_zip_code_prefix as INTEGER) as customer_zip_code_prefix,
    cast (regexp_extract(customer_id,r'^.{0,3}') as STRING) as customer_id,
    cast (customer_unique_id as STRING) as customer_unique_id,
    cast (customer_city as STRING) as customer_city,
    cast (customer_state as STRING) as customer_state,
    cast (dex_ingestion_timestamp as TIMESTAMP) as dex_ingestion_timestamp
FROM 
    {{source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_customers')}}

