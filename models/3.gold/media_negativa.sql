select
    ROUND((SUM(CASE WHEN avg_review_score <= 3 THEN 1 ELSE 0 END) / COUNT(*)), 4) as negative_reviews_percentage
from
    {{ ref('nota_do_produto') }}