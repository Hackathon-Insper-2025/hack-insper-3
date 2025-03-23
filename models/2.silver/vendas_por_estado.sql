SELECT 
    c.customer_state,
    SUM(oi.price) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(oi.order_item_id) AS total_items_sold,
    AVG(oi.price) AS avg_price_per_item
FROM 
    {{ref('orders')}} AS o
JOIN 
    {{ref('customers')}} AS c 
    ON o.customer_id = c.customer_id
JOIN 
    {{ref('order_items')}} AS oi 
    ON o.order_id = oi.order_id
GROUP BY 
    c.customer_state
ORDER BY 
    total_revenue DESC