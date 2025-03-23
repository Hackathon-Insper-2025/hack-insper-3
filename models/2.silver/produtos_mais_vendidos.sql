SELECT 
    p.product_id,
    p.product_category_name,
    pcnt.product_category_name_english,
    COUNT(oi.order_item_id) AS total_items_sold,
    SUM(oi.price) AS total_revenue,
    AVG(oi.price) AS avg_price_per_item
FROM 
    {{ref('order_items')}} AS oi
JOIN 
    {{ref('products')}} p ON oi.product_id = p.product_id
LEFT JOIN 
    {{ref('product_category_name_translation')}} pcnt ON p.product_category_name = pcnt.product_category_name
WHERE
    pcnt.product_category_name_english != 'null'
GROUP BY 
    p.product_id,
    p.product_category_name,
    pcnt.product_category_name_english
ORDER BY 
    total_items_sold DESC
