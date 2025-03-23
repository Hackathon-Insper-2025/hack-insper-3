SELECT
    cast (geolocation_zip_code_prefix as INTEGER) as geolocation_zip_code_prefix,
    cast (geolocation_lat as FLOAT64) as geolocation_lat,
    cast (geolocation_lng as FLOAT64) as geolocation_lng,
    cast (geolocation_city as STRING) as geolocation_city,
    cast (geolocation_state as STRING) as geolocation_state,
    cast (dex_ingestion_timestamp as TIMESTAMP) as dex_ingestion_timestamp
FROM 
    {{ source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_geolocation') }}