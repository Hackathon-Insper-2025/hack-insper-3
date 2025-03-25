with items_per_order as (
    select
        o.order_id,
        count(oi.order_item_id) as item_count
    from
        {{ ref('orders') }} o
    join
        {{ ref('order_items') }} oi on o.order_id = oi.order_id
    where
        o.order_status = 'delivered'
    group by
        o.order_id
)

select
    ROUND(count(case when item_count > 1 then order_id end) * 100.0 / count(order_id), 2) as percent_orders_with_multiple_items
from
    items_per_order