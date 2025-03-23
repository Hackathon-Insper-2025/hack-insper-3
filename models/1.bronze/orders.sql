select
    cast (order_id as STRING) as order_id,
    cast (regexp_extract(customer_id,r'^.{0,3}') as STRING) as customer_id,
    cast (order_status as STRING) as order_status,
    cast (order_purchase_timestamp as STRING) as order_purchase_timestamp,
    cast (order_approved_at as STRING) as order_approved_at,
    cast (order_delivered_carrier_date as STRING) as order_delivered_carrier_date,
    cast (order_delivered_customer_date as STRING) as order_delivered_customer_date,
    cast (order_estimated_delivery_date as STRING) as order_estimated_delivery_date,
    cast (dex_ingestion_timestamp as TIMESTAMP) as dex_ingestion_timestamp
from 
    {{ source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_orders') }}