select
    ROUND(count(oi.order_item_id) * 1.0 / count(distinct oi.order_id), 2) as avg_items_per_order
from
    {{ ref('order_items') }} oi
join
    {{ ref('orders') }} o on oi.order_id = o.order_id
where
    o.order_status = 'delivered'