select
    DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp), MONTH)) as order_month,
    count(order_id) as total_orders
from
    {{ ref('orders') }}
where
    order_purchase_timestamp is not null
    and order_purchase_timestamp != ''
    and PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp) <= TIMESTAMP('2018-08-31 23:59:59')
group by
    DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp), MONTH))
order by
    order_month asc