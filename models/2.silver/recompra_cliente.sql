with customer_orders as (
    select
        customer_id,
        count(order_id) as total_orders
    from
        {{ ref('orders') }} o
    where
        order_purchase_timestamp is not null
        and order_purchase_timestamp != ''
    group by
        customer_id
)

select
    ROUND(
        (count(case when total_orders > 1 then 1 end) / CAST(count(customer_id) as FLOAT64)) * 100,
        2
    ) as repurchase_rate,
    avg(total_orders) as avg_orders_per_customer,
    count(distinct customer_id) as total_customers,
    count(case when total_orders > 1 then 1 end) as returning_customers
from
    customer_orders