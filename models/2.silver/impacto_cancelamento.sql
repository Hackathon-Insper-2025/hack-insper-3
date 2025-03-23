SELECT
    o.order_status, 
    EXP(AVG(LN(CASE WHEN oi.freight_value > 0 THEN oi.freight_value ELSE NULL END))) AS media_geometrica_frete,
    AVG(oi.price) AS media_preco, 
    COUNT(o.order_id) AS numero_ordens
FROM
    {{ref('orders')}} o
JOIN
    {{ref('order_items')}} oi ON o.order_id = oi.order_id
WHERE
    o.order_status IN ('delivered', 'canceled')
GROUP BY
    o.order_status
