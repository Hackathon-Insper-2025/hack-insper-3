select
    order_month,
    total_orders,
    total_canceled_orders,
    ROUND((total_canceled_orders / total_orders) , 4) as canceled_orders_percentage
from
    {{ ref('cancelados_por_mes') }}
where
    total_orders > 0  -- Evita divisão por zero
order by
    order_month desc