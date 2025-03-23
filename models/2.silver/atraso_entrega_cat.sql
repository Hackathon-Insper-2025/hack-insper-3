select
    p.product_category_name as product_category,
    count(case when PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_customer_date) > 
                       PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_estimated_delivery_date) 
          then 1 end) as delayed_orders,
    ROUND(
        avg(case when PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_customer_date) > 
                         PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_estimated_delivery_date) 
                 then TIMESTAMP_DIFF(
                          PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_customer_date),
                          PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_estimated_delivery_date),
                          DAY
                      ) 
                 end), 2
    ) as avg_delay_days
from
    {{ ref('orders') }} o
join
    {{ ref('order_items') }} oi on o.order_id = oi.order_id
join
    {{ ref('products') }} p on oi.product_id = p.product_id
where
    p.product_category_name != ''
    and order_delivered_customer_date != ''
    and order_estimated_delivery_date != ''

group by
    product_category
order by
    avg_delay_days