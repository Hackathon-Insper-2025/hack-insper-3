select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.geolocation_lat,
    g.geolocation_lng,
    g.geolocation_city,
    g.geolocation_state
from
    {{ ref('customers') }} c
left join
    {{ ref('geolocation') }} g
    on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
where
    c.customer_id is not null
    and c.customer_zip_code_prefix is not null