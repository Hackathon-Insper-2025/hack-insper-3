select
    p.product_category_name as product_category,
    count(distinct o.order_id) as total_orders
from
    {{ ref('orders') }} o
join
    {{ ref('order_items') }} oi on o.order_id = oi.order_id
join
    {{ ref('products') }} p on oi.product_id = p.product_id
where
    p.product_category_name is not null
    and p.product_category_name != ''
group by
    p.product_category_name