select
    aec.order_month,
    aec.product_category,
    ROUND((SUM(aec.delayed_orders) / SUM(toc.total_orders)), 4) as delay_rate
from
    {{ ref('atraso_entrega_cat') }} aec
join
    {{ ref('total_orders_by_category') }} toc
    on aec.product_category = toc.product_category
    and aec.order_month = toc.order_month
where
    toc.total_orders > 0
    and not (EXTRACT(YEAR from aec.order_month) = 2018 
             and EXTRACT(MONTH from aec.order_month) in (9, 10))
group by
    aec.order_month,
    aec.product_category
order by
    aec.order_month asc,
    aec.product_category asc