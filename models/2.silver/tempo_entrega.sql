select
    p.product_category_name as product_category,
    ROUND(
        avg(
            TIMESTAMP_DIFF(
                PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_delivered_customer_date),
                PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp),
                DAY
            )
        ), 2
    ) as avg_delivery_time_days
from
    {{ ref('orders') }} o
join
    {{ ref('order_items') }} oi on o.order_id = oi.order_id
join
    {{ ref('products') }} p on oi.product_id = p.product_id
where
    o.order_delivered_customer_date is not null
    and o.order_purchase_timestamp is not null
    and o.order_delivered_customer_date != ''
    and o.order_purchase_timestamp != ''
group by
    p.product_category_name
order by
    avg_delivery_time_days desc