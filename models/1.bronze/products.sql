SELECT 
    product_name_lenght as product_name_lenght,
    product_description_lenght as product_description_lenght,
    product_photos_qty as product_photos_qty,
    product_weight_g as product_weight_g,
    product_length_cm as product_length_cm,
    product_height_cm as product_height_cm,
    product_width_cm as product_width_cm,
    product_id as product_id,
    product_category_name as product_category_name,
    dex_ingestion_timestamp as dex_ingestion_timestamp
FROM 
    {{source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_products')}}