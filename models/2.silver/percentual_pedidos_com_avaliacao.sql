select
    ROUND(count(distinct orr.order_id) * 100.0 / count(distinct o.order_id), 2) as percent_orders_with_review
from
    {{ ref('orders') }} o
left join
    {{ ref('orders_reviews') }} orr on o.order_id = orr.order_id
where
    o.order_status = 'delivered'