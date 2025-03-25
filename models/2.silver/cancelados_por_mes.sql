select
    DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp), MONTH)) as order_month,
    COUNT(*) as total_orders,
    SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) as total_canceled_orders
from
    {{ ref('orders') }}
where
    order_purchase_timestamp is not null
    and order_purchase_timestamp != ''
group by
    DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp), MONTH))
order by
    order_month desc