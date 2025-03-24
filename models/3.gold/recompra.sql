select
    repurchase_rate,
    avg_orders_per_customer,
    total_customers,
    returning_customers
from
    {{ref('recompra_cliente')}}