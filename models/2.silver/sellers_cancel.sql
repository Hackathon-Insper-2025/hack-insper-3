select
    s.seller_id,
    count(o.order_id) as total_orders,
    count(case when o.order_status = 'canceled' then 1 end) as canceled_orders,
    ROUND(
        (count(case when o.order_status = 'canceled' then 1 end) / CAST(count(o.order_id) as FLOAT64)) * 100, 
        2
    ) as cancel_percent
from
    {{ ref('sellers') }} s
join
    {{ ref('order_items') }} oi on s.seller_id = oi.seller_id
join
    {{ ref('orders') }} o on oi.order_id = o.order_id
group by
    s.seller_id
order by
    cancel_percent desc