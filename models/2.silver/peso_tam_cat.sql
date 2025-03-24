select
    product_category_name as product_category,
    ROUND(AVG(product_weight_g), 2) as product_weight, 
    ROUND(AVG(product_length_cm), 2) as product_length, 
    ROUND(AVG(product_height_cm), 2) as product_height, 
    ROUND(AVG(product_width_cm), 2) as product_width
from
    {{ ref('products') }}
where
    product_category_name != ''
group by
    product_category