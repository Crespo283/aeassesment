SELECT DISTINCT
    appid,
    supported_languages
FROM {{ ref('stg_application_supported_languages') }}
WHERE supported_languages IS NOT NULL -- The NULL data is irrelevant
GROUP BY 1, 2