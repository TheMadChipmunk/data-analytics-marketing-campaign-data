SELECT
    products_id,
    CAST(purchSE_PRICE AS DOUBLE)  AS purchase_price
FROM {{ source('raw', 'raw_gz_product') }}
WHERE products_id IS NOT NULL
