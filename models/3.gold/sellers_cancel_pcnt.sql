select
    ROUND((SUM(CASE WHEN canceled_orders > 0 THEN 1 ELSE 0 END) / COUNT(*)), 4) as sellers_with_cancellations_percentage
from
    {{ ref('sellers_cancel') }}