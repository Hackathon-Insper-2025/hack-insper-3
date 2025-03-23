SELECT 
    cast (product_name_lenght as FLOAT64) as product_name_lenght,
    cast (product_description_lenght as FLOAT64) as product_description_lenght,
    cast (product_photos_qty as FLOAT64) as product_photos_qty,
    cast (product_weight_g as FLOAT64) as product_weight_g,
    cast (product_length_cm as FLOAT64) as product_length_cm,
    cast (product_height_cm as FLOAT64) as product_height_cm,
    cast (product_width_cm as FLOAT64) as product_width_cm,
    cast (product_id as STRING) as product_id,
    cast (product_category_name as STRING) as product_category_name,
    cast (dex_ingestion_timestamp as TIMESTAMP) as dex_ingestion_timestamp
FROM 
    {{source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_products')}}