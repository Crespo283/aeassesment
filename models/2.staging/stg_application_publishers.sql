SELECT
    id AS appid,
    publisher
FROM {{ ref('src_application_publishers') }}