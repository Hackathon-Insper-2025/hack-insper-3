with first_purchase as (
    select
        c.customer_id,
        min(DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp), MONTH))) as first_purchase_month
    from
        {{ ref('orders') }} o
    join
        {{ ref('customers') }} c on o.customer_id = c.customer_id
    where
        o.order_purchase_timestamp is not null
        and o.order_purchase_timestamp != ''
        and o.order_status = 'delivered'
    group by
        c.customer_id
),

all_months as (
    select distinct
        DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp), MONTH)) as order_month
    from
        {{ ref('orders') }}
    where
        order_purchase_timestamp is not null
        and order_purchase_timestamp != ''
        and not (EXTRACT(YEAR from PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp)) = 2018
                 and EXTRACT(MONTH from PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp)) in (9, 10))
),

cumulative_customers as (
    select
        am.order_month,
        count(distinct fp.customer_id) as total_customers
    from
        all_months am
    left join
        first_purchase fp on fp.first_purchase_month <= am.order_month
    group by
        am.order_month
)

select
    order_month,
    total_customers
from
    cumulative_customers
order by
    order_month asc