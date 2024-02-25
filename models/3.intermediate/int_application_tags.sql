SELECT DISTINCT
    appid,
    tag
FROM {{ ref('stg_application_tags') }}
WHERE tag IS NOT NULL-- The NULL data is irrelevant
GROUP BY 1, 2