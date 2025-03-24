select
    p.product_photos_qty as photos_quantity,
    count(distinct oi.order_id) as total_orders
from
    {{ ref('products') }} p
left join
    {{ ref('order_items') }} oi on p.product_id = oi.product_id
where
    p.product_photos_qty is not null
group by
    p.product_photos_qty
order by
    photos_quantity asc