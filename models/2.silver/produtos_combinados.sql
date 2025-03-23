SELECT 
    oi1.product_id AS product_id_1,
    p1.product_category_name AS category_1,
    oi2.product_id AS product_id_2,
    p2.product_category_name AS category_2,
    COUNT(DISTINCT oi1.order_id) AS times_sold_together,
    SUM(oi1.price + oi2.price) AS total_revenue_together
FROM 
    {{ref('order_items')}} AS oi1
JOIN 
    {{ref('order_items')}} AS oi2 
    ON oi1.order_id = oi2.order_id 
    AND oi1.product_id < oi2.product_id
JOIN 
    {{ref('products')}} AS p1 
    ON oi1.product_id = p1.product_id
JOIN 
    {{ref('products')}} AS p2 
    ON oi2.product_id = p2.product_id
GROUP BY 
    oi1.product_id,
    p1.product_category_name,
    oi2.product_id,
    p2.product_category_name
ORDER BY 
    times_sold_together DESC