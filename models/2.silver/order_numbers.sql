SELECT
    o.order_status AS order_status,
    EXTRACT(YEAR FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_purchase_timestamp)) AS order_year,
    COUNT(DISTINCT o.order_id) AS order_count
FROM
    {{ ref('orders') }} o
GROUP BY
    order_status,
    order_year
ORDER BY
    order_status ASC,
    order_year ASC