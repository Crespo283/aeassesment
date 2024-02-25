SELECT DISTINCT
    appid,
    publisher
FROM {{ ref('stg_application_publishers') }}
WHERE publisher IS NOT NULL -- The NULL data is irrelevant
GROUP BY 1, 2