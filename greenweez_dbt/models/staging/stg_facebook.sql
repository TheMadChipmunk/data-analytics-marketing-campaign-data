SELECT
    date_date,
    campaign_key,
    "camPGN_name"              AS campaign_name,
    CAST(ads_cost AS DOUBLE)   AS ads_cost,
    impression                 AS ads_impressions,
    click                      AS ads_clicks,
    'facebook'                 AS platform
FROM {{ source('raw_marketing', 'raw_gz_facebook') }}
