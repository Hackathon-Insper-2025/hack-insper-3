SELECT 
    p.product_id,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    COUNT(oi.order_id) AS numero_de_cancelamentos
FROM
    {{ref('order_items')}} oi
JOIN
    {{ref('orders')}} o ON oi.order_id = o.order_id
JOIN
    {{ref('products')}} p ON oi.product_id = p.product_id
WHERE
    o.order_status = 'canceled'
GROUP BY
    p.product_id, p.product_weight_g, p.product_length_cm, p.product_height_cm, p.product_width_cm
ORDER BY
    numero_de_cancelamentos DESC
