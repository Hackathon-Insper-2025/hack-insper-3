select
    p.product_id,
    ROUND(AVG(orv.review_score), 2) as avg_review_score
from
    {{ ref('orders_reviews') }} orv
join
    {{ ref('order_items') }} oi on orv.order_id = oi.order_id
join
    {{ ref('products') }} p on oi.product_id = p.product_id
where
    orv.review_score is not null
group by
    p.product_id