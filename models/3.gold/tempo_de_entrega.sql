with delayed_with_percentage as (
    select
        a.product_category,
        a.delayed_orders,
        a.avg_delay_days,
        toc.total_orders,
        ROUND((a.delayed_orders / toc.total_orders), 4) as delayed_orders_percentage
    from
        {{ ref('atraso_entrega_cat') }} a
    join
        {{ ref('total_orders_by_category') }} toc
        on a.product_category = toc.product_category
)

select
    t.product_category,
    t.avg_delivery_time_days,
    dwp.delayed_orders_percentage,
    dwp.avg_delay_days
from
    {{ ref('tempo_entrega') }} t
join
    delayed_with_percentage dwp
    on t.product_category = dwp.product_category
order by
    dwp.delayed_orders_percentage desc