SELECT DISTINCT
    appid,
    package
FROM {{ ref('stg_application_packages')}}
WHERE package IS NOT NULL -- The NULL data is irrelevant
GROUP BY 1, 2
