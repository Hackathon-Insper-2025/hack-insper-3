with monthly_orders as (
    select
        p.product_category_name as product_category,
        DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp), MONTH)) as order_month,
        count(distinct o.order_id) as orders_count
    from
        {{ ref('orders') }} o
    join
        {{ ref('order_items') }} oi on o.order_id = oi.order_id
    join
        {{ ref('products') }} p on oi.product_id = p.product_id
    where
        o.order_purchase_timestamp is not null
        and o.order_purchase_timestamp != ''
        and o.order_status = 'delivered'
    group by
        p.product_category_name,
        DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp), MONTH))
),

moving_average as (
    select
        product_category,
        order_month,
        orders_count,
        ROUND(
            avg(orders_count) over (
                partition by product_category
                order by order_month
                rows between 2 preceding and current row
            ), 2
        ) as moving_avg_3_months
    from
        monthly_orders
)

select
    product_category,
    order_month,
    orders_count,
    moving_avg_3_months
from
    moving_average
where
    order_month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH)
order by
    product_category,
    order_month desc