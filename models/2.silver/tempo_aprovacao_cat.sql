select
    p.product_category_name as product_category,
    round(
        avg(
            TIMESTAMP_DIFF(
                PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_approved_at),
                PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp),
                HOUR
            )
        ), 2
    ) as avg_approval_time_hours
from
    {{ ref('orders') }} o
join
    {{ ref('order_items') }} oi on o.order_id = oi.order_id
join
    {{ ref('products') }} p on oi.product_id = p.product_id
where
    p.product_category_name != ''
    and o.order_approved_at != ''
    and o.order_purchase_timestamp != ''
group by
    p.product_category_name
order by
    avg_approval_time_hours desc