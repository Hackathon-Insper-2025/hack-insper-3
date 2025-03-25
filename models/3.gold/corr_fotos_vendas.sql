select
    ROUND(CORR(photos_quantity, total_orders), 4) as correlation_photos_sales
from
    {{ ref('produtos_por_fotos') }}
where
    photos_quantity is not null and photos_quantity <= 5
    and total_orders is not null