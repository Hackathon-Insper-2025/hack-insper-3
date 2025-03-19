select
    payment_sequential as payment_sequential,
    payment_installments as payment_installments,
    payment_value as payment_value,
    order_id as order_id,
    payment_type as payment_type,
    dex_ingestion_timestamp as dex_ingestion_timestamp
from 
    {{ source('dex-dsm-production-dex_landing', 'insper_data_3__postgres_ecommerce_db_order_payments') }}