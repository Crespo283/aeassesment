SELECT DISTINCT
    appid,
    type,
    name,
    releasedate,
    freetoplay,
    developer,
    publisher,
    genre,
    supported_languages,
    tag
FROM {{ ref('int_application_information') }}