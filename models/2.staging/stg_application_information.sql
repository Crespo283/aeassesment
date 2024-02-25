SELECT
    appid,
    type,
    name,
    releasedate,
    freetoplay
FROM {{ ref('src_application_information') }}