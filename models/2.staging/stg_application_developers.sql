SELECT
    id AS appid, -- In application_information appears as appid
    developer
FROM {{ref('src_application_developers')}}