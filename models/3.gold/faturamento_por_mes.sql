select
    order_month as data_compra,
    total_revenue as valor_vendido
    from 
        {{ ref('faturamento_mes') }}