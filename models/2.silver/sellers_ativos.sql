select
    count(distinct s.seller_id) as total_active_sellers
from
    {{ ref('sellers') }} s
join
    {{ ref('order_items') }} oi on s.seller_id = oi.seller_id
join
    {{ ref('orders') }} o on oi.order_id = o.order_id
where
    o.order_status = 'delivered'