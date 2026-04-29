SELECT
    m.date_date,
    m.orders_id,
    m.products_id,
    m.revenue,
    m.quantity,
    m.purchase_price,
    m.purchase_cost,
    m.margin,
    sh.ship_cost,
    m.margin - COALESCE(sh.ship_cost,0) AS operational_margin
FROM {{ ref('int_orders_margin') }} m
LEFT JOIN {{ ref('stg_ship') }} sh
    ON m.orders_id = sh.orders_id
