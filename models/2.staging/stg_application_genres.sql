WITH genres AS (
    SELECT
        id AS appid,
        genre_1,
        genre_2,
        genre_3
    FROM {{ ref('src_application_genres') }}
)

, base AS (
{{ union_columns('genres', 'appid', ['genre_1', 'genre_2', 'genre_3']) }} -- This macro unifies all the columns of the same type
)

SELECT 
    appid,
    unified_column AS genre
FROM base