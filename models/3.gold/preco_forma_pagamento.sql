select
    op.payment_type,
    ROUND(
        avg(op.payment_value), 2
    ) as avg_payment_value,
    count(distinct op.order_id) as total_orders
from
    {{ ref('order_payments') }} op
join
    {{ ref('orders') }} o on op.order_id = o.order_id
where
    op.payment_type != 'not_defined'
group by
    op.payment_type
order by
    avg_payment_value desc