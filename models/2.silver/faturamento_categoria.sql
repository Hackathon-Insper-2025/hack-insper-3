SELECT 
    p.product_category_name,
    pcnt.product_category_name_english,
    SUM(oi.price) AS total_revenue,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(oi.order_item_id) AS total_items_sold
FROM 
    {{ref('order_items')}} AS oi
JOIN 
    {{ref('products')}} p ON oi.product_id = p.product_id
LEFT JOIN 
    {{ref('product_category_name_translation')}} pcnt ON p.product_category_name = pcnt.product_category_name
GROUP BY 
    p.product_category_name,
    pcnt.product_category_name_english
ORDER BY 
    total_revenue DESC