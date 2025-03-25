select
    c.customer_id,
    ROUND(avg(op.payment_value), 2) as avg_spend_per_customer,
    count(distinct o.order_id) as total_orders,
    ROUND(count(distinct o.order_id) * 1.0 / count(distinct c.customer_id), 2) as avg_orders_per_customer
from
    {{ ref('customers') }} c
join
    {{ ref('orders') }} o on c.customer_id = o.customer_id
join
    {{ ref('order_payments') }} op on o.order_id = op.order_id
where
    o.order_status = 'delivered'
    and op.payment_value is not null
group by
    c.customer_id
order by
    avg_spend_per_customer desc