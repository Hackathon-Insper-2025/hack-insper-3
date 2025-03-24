select
    ROUND(CORR(numero_de_cancelamentos, product_weight_g), 4) as corr_weight_cancel,
    ROUND(CORR(numero_de_cancelamentos, product_length_cm), 4) as corr_length_cancel,
    ROUND(CORR(numero_de_cancelamentos, product_height_cm), 4) as corr_height_cancel
from
    {{ ref('cancelamento_clientes') }}