select
    s.seller_id,
    ROUND(
        avg(CAST(r.review_score as FLOAT64)), 2
    ) as avg_seller_score,
    count(o.order_id) as total_orders,
    count(case when o.order_status = 'canceled' then 1 end) as canceled_orders,
    count(case when o.order_status = 'unavailable' then 1 end) as unavailable_orders
from
    {{ ref('sellers') }} s
join
    {{ ref('order_items') }} oi on s.seller_id = oi.seller_id
left join
    {{ ref('orders_reviews') }} r on oi.order_id = r.order_id
left join
    {{ ref('orders') }} o on oi.order_id = o.order_id
where
    r.review_score is not null
group by
    s.seller_id
order by
    avg_seller_score desc