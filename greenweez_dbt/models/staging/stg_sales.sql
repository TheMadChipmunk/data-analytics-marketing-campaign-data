SELECT
    date_date,
    orders_id,
    pdt_id      AS products_id,
    revenue,
    quantity
FROM {{ source('raw', 'raw_gz_sales') }}
WHERE revenue IS NOT NULL
  AND quantity > 0
