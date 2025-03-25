select
    p.product_category_name as product_category,
    ROUND(sum(op.payment_value) / count(distinct op.order_id), 2) as avg_ticket_per_category
from
    {{ ref('order_payments') }} op
join
    {{ ref('order_items') }} oi on op.order_id = oi.order_id
join
    {{ ref('products') }} p on oi.product_id = p.product_id
where
    op.payment_value is not null
group by
    p.product_category_name
order by
    avg_ticket_per_category desc