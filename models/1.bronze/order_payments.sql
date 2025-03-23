select
    cast (payment_sequential as INTEGER) as payment_sequential,
    cast (payment_installments as INTEGER) as payment_installments,
    cast (payment_value as FLOAT64) as payment_value,
    cast (order_id as STRING) as order_id,
    cast (payment_type as STRING) as payment_type,
    cast (dex_ingestion_timestamp as TIMESTAMP) as dex_ingestion_timestamp
from 
    {{ source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_order_payments') }}