SELECT
    date_date,
    COUNT(DISTINCT orders_id)                                           AS nb_transactions,
    SUM(revenue)                                                        AS revenue,
    ROUND(SUM(revenue) / NULLIF(COUNT(DISTINCT orders_id), 0), 2)      AS average_basket,
    SUM(purchase_cost)                                                  AS purchase_cost,
    SUM(margin)                                                         AS margin,
    SUM(operational_margin)                                             AS operational_margin,
    ROUND(SUM(margin) / NULLIF(SUM(revenue), 0) * 100, 2)              AS margin_percent,
    ROUND(SUM(operational_margin) / NULLIF(SUM(revenue), 0) * 100, 2)  AS operational_margin_percent
FROM {{ ref('int_orders_operational') }}
GROUP BY date_date
ORDER BY date_date
