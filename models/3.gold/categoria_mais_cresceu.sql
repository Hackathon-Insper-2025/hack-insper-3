with recent_months as (
    select
        order_month,
        product_category_name,
        total_revenue,
        DENSE_RANK() over (order by order_month desc) as month_rank
    from
        {{ ref('faturamento_cat_mes') }}
),

previous_and_antepenultimate_month as (
    select
        order_month,
        product_category_name,
        total_revenue,
        month_rank
    from
        recent_months
    where
        month_rank in (3, 4)  -- 3 é o antepenúltimo mês, 4 é o mês anterior (quarto mais recente)
),

revenue_pivot as (
    select
        product_category_name,
        max(case when month_rank = 4 then total_revenue else 0 end) as revenue_previous_month,
        max(case when month_rank = 3 then total_revenue else 0 end) as revenue_antepenultimate_month,
        max(case when month_rank = 4 then order_month end) as previous_month,
        max(case when month_rank = 3 then order_month end) as antepenultimate_month
    from
        previous_and_antepenultimate_month
    group by
        product_category_name
),

growth_calc as (
    select
        product_category_name,
        previous_month,
        antepenultimate_month,
        revenue_previous_month,
        revenue_antepenultimate_month,
        ROUND(revenue_antepenultimate_month - revenue_previous_month, 2) as absolute_difference,
        case 
            when revenue_previous_month = 0 then null  -- Evita divisão por zero
            else ROUND(((revenue_antepenultimate_month - revenue_previous_month) / revenue_previous_month) , 4)
        end as percentage_difference
    from
        revenue_pivot
    where
        revenue_previous_month > 0  -- Garante que o mês anterior tenha faturamento
        and revenue_antepenultimate_month > 0  -- Garante que o antepenúltimo mês tenha faturamento
)

select
    product_category_name,
    previous_month,
    antepenultimate_month,
    revenue_previous_month,
    revenue_antepenultimate_month,
    absolute_difference,
    percentage_difference
from
    growth_calc
order by
    percentage_difference desc