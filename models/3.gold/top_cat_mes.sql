with top_categories as (
    select
        product_category_name,
        sum(total_revenue) as total_revenue_category
    from
        {{ ref('faturamento_cat_mes') }}
    group by
        product_category_name
    order by
        total_revenue_category desc
    limit 5
),

last_three_months as (
    select
        EXTRACT(YEAR from order_month) as year,
        EXTRACT(MONTH from order_month) as month
    from
        {{ ref('faturamento_cat_mes') }}
    group by
        EXTRACT(YEAR from order_month),
        EXTRACT(MONTH from order_month)
    order by
        EXTRACT(YEAR from order_month) desc,
        EXTRACT(MONTH from order_month) desc
    limit 5
),

faturamento_filtered as (
    select
        EXTRACT(YEAR from fcm.order_month) as year,
        EXTRACT(MONTH from fcm.order_month) as month,
        fcm.product_category_name,
        fcm.total_revenue
    from
        {{ ref('faturamento_cat_mes') }} fcm
    join
        top_categories tc
        on fcm.product_category_name = tc.product_category_name
    join
        last_three_months lsm
        on EXTRACT(YEAR from fcm.order_month) = lsm.year
        and EXTRACT(MONTH from fcm.order_month) = lsm.month
)

select
    year,
    month,
    product_category_name,
    total_revenue
from
    faturamento_filtered
order by
    year asc,
    month asc,
    total_revenue desc