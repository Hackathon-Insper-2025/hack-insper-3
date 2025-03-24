select
    a.seller_id,
    a.avg_seller_score,
    a.canceled_orders,
    c.cancel_percent
from
    {{ ref('avg_seller') }} a
join
    {{ ref('sellers_cancel') }} c on a.seller_id = c.seller_id
order by
    a.avg_seller_score desc