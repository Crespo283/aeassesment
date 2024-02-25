WITH information AS (
SELECT DISTINCT
    appid,
    type,
    name,
    releasedate,
    freetoplay
FROM {{ ref('stg_application_information') }}
WHERE type IS NOT NULL
    AND name IS NOT NULL
    AND releasedate IS NOT NULL
    AND freetoplay IS NOT NULL -- The NULL data is irrelevant
)

SELECT DISTINCT
    ai.appid,
    ai.type,
    ai.name,
    ai.releasedate,
    ai.freetoplay,
    ad.developer,
    ap.publisher,
    ag.genre,
    asl.supported_languages,
    atg.tag
FROM information AS ai
LEFT OUTER JOIN {{ ref('int_application_developers') }} AS ad ON ai.appid = ad.appid
LEFT OUTER JOIN {{ ref('int_application_publishers') }} AS ap ON ai.appid = ap.appid
LEFT OUTER JOIN {{ ref('int_application_genres') }} AS ag ON ai.appid = ag.appid
LEFT OUTER JOIN {{ ref('int_application_tags') }} AS atg ON ai.appid = atg.appid
LEFT OUTER JOIN {{ ref('int_application_supported_languages') }} AS asl ON ai.appid = asl.appid