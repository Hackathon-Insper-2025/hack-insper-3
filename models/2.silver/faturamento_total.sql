SELECT 
    EXTRACT(YEAR FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp)) AS year,
    SUM(op.payment_value) AS total_revenue
FROM 
    {{ ref('order_payments') }} op
JOIN 
    {{ref('orders')}} o ON op.order_id = o.order_id
GROUP BY 
    EXTRACT(YEAR FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp))
ORDER BY 
    year ASC