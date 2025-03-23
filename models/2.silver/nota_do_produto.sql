WITH order_ratings AS (
    SELECT 
        order_id,
        AVG(review_score) AS avg_order_rating,
        COUNT(review_score) AS num_reviews_per_order
    FROM 
        {{ref('orders_reviews')}}
    WHERE
        review_score >= 1
    GROUP BY 
        order_id
)

SELECT 
    p.product_id,
    p.product_category_name,
    AVG(orr.avg_order_rating) AS avg_product_rating,
    SUM(orr.num_reviews_per_order) AS total_reviews,
    MIN(orr.avg_order_rating) AS min_rating,
    MAX(orr.avg_order_rating) AS max_rating
FROM 
    {{ref('products')}} AS p
JOIN 
    {{ref('order_items')}} AS oi 
    ON p.product_id = oi.product_id
JOIN 
    order_ratings AS orr
    ON oi.order_id = orr.order_id
GROUP BY 
    p.product_id,
    p.product_category_name
ORDER BY 
    avg_product_rating DESC, total_reviews DESC