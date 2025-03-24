with recent_months as (
    select
        EXTRACT(YEAR from order_month) as year,
        EXTRACT(MONTH from order_month) as month,
        product_category_name,
        total_revenue,
        DENSE_RANK() over (order by EXTRACT(YEAR from order_month) desc, EXTRACT(MONTH from order_month) desc) as month_rank
    from
        {{ ref('faturamento_cat_mes') }}
),

previous_and_antepenultimate_month as (
    select
        product_category_name,
        year,
        month,
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
        max(case when month_rank = 4 then year end) as previous_year,
        max(case when month_rank = 4 then month end) as previous_month,
        max(case when month_rank = 3 then year end) as antepenultimate_year,
        max(case when month_rank = 3 then month end) as antepenultimate_month
    from
        previous_and_antepenultimate_month
    group by
        product_category_name
),

growth_calc as (
    select
        product_category_name,
        revenue_previous_month,
        revenue_antepenultimate_month,
        previous_year,
        previous_month,
        antepenultimate_year,
        antepenultimate_month,
        case 
            when revenue_previous_month = 0 then null  -- Evita divisão por zero
            else ROUND(((revenue_antepenultimate_month - revenue_previous_month) / revenue_previous_month) * 100, 2)
        end as revenue_growth_percent
    from
        revenue_pivot
    where
        revenue_antepenultimate_month > 0  -- Garante que o antepenúltimo mês tenha vendas
)

select
    product_category_name,
    revenue_growth_percent,
    ROUND(revenue_antepenultimate_month, 2) as revenue_antepenultimate_month,
    ROUND(revenue_previous_month, 2) as revenue_previous_month,
    previous_year,
    previous_month,
    antepenultimate_year,
    antepenultimate_month
from
    growth_calc
where
    revenue_growth_percent is not null
order by
    revenue_growth_percent desc
limit 1