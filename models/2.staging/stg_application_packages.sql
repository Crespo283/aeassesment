WITH packages AS (
    SELECT
        id AS appid,
        package_1,
        package_2
    FROM {{ ref('src_application_packages') }}
)

, base AS (
{{ union_columns('packages', 'appid', ['package_1', 'package_2']) }} -- This macro unifies all the columns of the same type
)

SELECT
    appid,
    unified_column AS package
FROM base