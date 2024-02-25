SELECT DISTINCT
    appid,
    developer
FROM {{ ref('stg_application_developers') }}
WHERE developer IS NOT NULL -- The NULL data is irrelevant
GROUP BY 1, 2