select
    DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp), MONTH)) as order_month,
    ROUND(sum(op.payment_value), 2) as total_revenue
from
    {{ ref('orders') }} o
join
    {{ ref('order_payments') }} op on o.order_id = op.order_id
where
    o.order_purchase_timestamp is not null
    and o.order_purchase_timestamp != ''
    and op.payment_value is not null
group by
    DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp), MONTH))
order by
    order_month desc