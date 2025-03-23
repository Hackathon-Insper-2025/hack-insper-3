SELECT
    AVG(DATEDIFF(r.review_answer_timestamp, r.review_creation_date)) AS tempo_medio_resposta_dias
FROM
    {{ref('orders_reviews')}} r
WHERE
    r.review_answer_timestamp IS NOT NULL
    AND r.review_creation_date IS NOT NULL