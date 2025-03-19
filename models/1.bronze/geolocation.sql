SELECT
    geolocation_zip_code_prefix as geolocation_zip_code_prefix,
    geolocation_lat as geolocation_lat,
    geolocation_lng as geolocation_lng,
    geolocation_city as geolocation_city,
    geolocation_state as geolocation_state,
    dex_ingestion_timestamp as dex_ingestion_timestamp
FROM 
    {{ source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_geolocation') }}