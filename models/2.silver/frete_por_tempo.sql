select
    DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp), MONTH)) as order_month,
    ROUND(sum(oi.freight_value), 2) as sum_freight_value,
    ROUND(avg(oi.freight_value), 2) as avg_freight_value
from
    {{ ref('orders') }} o
join
    {{ ref('order_items') }} oi on o.order_id = oi.order_id
where
    o.order_purchase_timestamp is not null
    and o.order_purchase_timestamp != ''
    and o.order_status = 'delivered'
    and oi.freight_value is not null
group by
    DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp), MONTH))
order by
    order_month desc