SELECT
    orders_id,
    CAST(ship_cost AS DOUBLE)  AS ship_cost
FROM {{ source('raw', 'raw_gz_ship') }}
