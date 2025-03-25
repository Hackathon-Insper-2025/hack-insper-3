select
    p.product_category_name as product_category,
    ROUND(AVG(orv.review_score), 2) as avg_review_score
from
    {{ ref('orders_reviews') }} orv
join
    {{ ref('order_items') }} oi on orv.order_id = oi.order_id
join
    {{ ref('products') }} p on oi.product_id = p.product_id
where
    p.product_category_name is not null
    and p.product_category_name != ''
    and orv.review_score is not null
group by
    p.product_category_name
order by
    avg_review_score desc