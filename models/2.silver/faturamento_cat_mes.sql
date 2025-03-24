with order_revenue as (
    select
        o.order_id,
        DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp), MONTH)) as order_month,
        ROUND(sum(op.payment_value), 2) as order_revenue
    from
        {{ ref('orders') }} o
    join
        {{ ref('order_payments') }} op on o.order_id = op.order_id
    where
        o.order_purchase_timestamp is not null
        and o.order_purchase_timestamp != ''
        and op.payment_value is not null
    group by
        o.order_id,
        DATE(DATE_TRUNC(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp), MONTH))
),

order_items_with_revenue as (
    select
        orr.order_id,
        orr.order_month,
        p.product_category_name,
        oi.price,
        orr.order_revenue,
        -- Calcula a proporção do price de cada item em relação ao total do pedido
        CASE
            WHEN sum(oi.price) over (partition by oi.order_id) > 0
            THEN (oi.price / sum(oi.price) over (partition by oi.order_id)) * orr.order_revenue
            ELSE orr.order_revenue  -- Se não houver itens ou price, atribui o faturamento total ao pedido
        END as proportional_revenue
    from
        order_revenue orr
    left join
        {{ ref('order_items') }} oi on orr.order_id = oi.order_id
    left join
        {{ ref('products') }} p on oi.product_id = p.product_id
)

select
    order_month,
    COALESCE(product_category_name, 'desconhecida') as product_category_name,
    ROUND(sum(proportional_revenue), 2) as total_revenue
from
    order_items_with_revenue
group by
    order_month,
    COALESCE(product_category_name, 'desconhecida')
order by
    order_month desc,
    total_revenue desc