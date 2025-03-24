select
    customer_id,
    geolocation_lat,
    geolocation_lng
from
    {{ ref('clientes_com_coordenadas') }}
where
    geolocation_lat is not null
    and geolocation_lng is not null
order by
    customer_id