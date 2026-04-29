SELECT
    s.date_date,
    s.orders_id,
    s.products_id,
    s.revenue,
    s.quantity,
    p.purchase_price,
    s.quantity * p.purchase_price                     AS purchase_cost,
    s.revenue - (s.quantity * p.purchase_price)       AS margin
FROM {{ ref('stg_sales') }} s
LEFT JOIN {{ ref('stg_product') }} p
    ON s.products_id = p.products_id
