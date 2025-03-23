select
    EXTRACT(YEAR from PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp)) as year,
    EXTRACT(MONTH from PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp)) as month,
    count(distinct customer_id) as unique_customers
from
    {{ ref('orders') }} o
where
    order_purchase_timestamp is not null
    and order_purchase_timestamp != ''
group by
    EXTRACT(YEAR from PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp)),
    EXTRACT(MONTH from PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp))
order by
    year asc,
    month asc