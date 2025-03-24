select
    order_month,
    sum(total_revenue) as total_revenue
from
    {{ ref('faturamento_cat_mes') }}
group by
    order_month
order by
    order_month desc