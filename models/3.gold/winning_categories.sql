with avg_revenue as (
    select
        avg(total_revenue) as avg_category_revenue
    from
        {{ ref('faturamento_categoria') }}
)

select
    fc.product_category_name,
    fc.product_category_name_english,
    fc.total_revenue,
    fc.total_orders,
    fc.total_items_sold
from
    {{ ref('faturamento_categoria') }} fc,
    avg_revenue ar
where
    fc.total_revenue > ar.avg_category_revenue
order by
    fc.total_revenue desc