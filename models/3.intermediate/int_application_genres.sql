SELECT DISTINCT
    appid,
    genre
FROM {{ ref('stg_application_genres') }}
WHERE genre IS NOT NULL -- The NULL data is irrelevant
GROUP BY 1, 2