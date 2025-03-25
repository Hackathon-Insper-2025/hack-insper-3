with last_twelve_months as (
    select
        year,
        month
    from
        {{ ref('total_orders') }}
    group by
        year,
        month
    order by
        year desc,
        month desc
    limit 15
)

select
    CONCAT(
        CAST(ord.year as STRING),
        '-',
        LPAD(CAST(ord.month as STRING), 2, '0')
    ) as year_month,
    ord.total_orders
from
    {{ ref('total_orders') }} ord
join
    last_twelve_months ltm
    on ord.year = ltm.year
    and ord.month = ltm.month
order by
    ord.year asc,
    ord.month asc