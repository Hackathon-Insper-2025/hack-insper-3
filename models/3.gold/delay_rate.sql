select
    ROUND((SUM(aec.delayed_orders) / SUM(toc.total_orders)), 4) as delay_rate
from
    {{ ref('atraso_entrega_cat') }} aec
join
    {{ ref('total_orders_by_category') }} toc
    on aec.product_category = toc.product_category
where
    toc.total_orders > 0