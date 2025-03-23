SELECT 
    p.product_id,
    p.product_category_name,
    COUNT(DISTINCT o.order_id) AS total_canceled_orders,
    COUNT(oi.order_item_id) AS total_canceled_items,
    SUM(oi.price) AS total_canceled_revenue
FROM 
    {{ref('orders')}} AS o
JOIN 
    {{ref('order_items')}} AS oi 
    ON o.order_id = oi.order_id
JOIN 
    {{ref('products')}} AS p 
    ON oi.product_id = p.product_id
WHERE 
    o.order_status = 'canceled'
GROUP BY 
    p.product_id,
    p.product_category_name
ORDER BY 
    total_canceled_orders DESC