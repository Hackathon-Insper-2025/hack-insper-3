SELECT
    TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', r.review_answer_timestamp),
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', r.review_creation_date),
        DAY
    ) AS tempo_resposta_dias,
    r.review_score AS satisfacao_cliente
FROM
    {{ref('orders_reviews')}} r
WHERE
    r.review_answer_timestamp IS NOT NULL
    AND r.review_creation_date IS NOT NULL
    AND r.review_answer_timestamp != ''
    AND r.review_creation_date != ''
ORDER BY
    r.review_score DESC
LIMIT 10  
