SELECT
    cast (order_item_id as INTEGER) as order_item_id,
    cast (price as FLOAT64) as price,
    cast (freight_value as FLOAT64) as freight_value,
    cast (order_id as STRING) as order_id,
    cast (product_id as STRING) as product_id,
    cast (regexp_extract(seller_id,r'^.{0,2}') as STRING) as seller_id,
    cast (shipping_limit_date as STRING) as shipping_limit_date,
    cast (dex_ingestion_timestamp as TIMESTAMP) as dex_ingestion_timestamp
FROM
    {{source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_order_items')}}