WITH languages AS (
    SELECT
        id AS appid,
        language_1,
        language_2,
        language_3,
        language_4,
        language_5,
        language_6,
        language_7,
        language_8,
        language_9,
        language_10,
        language_11,
        language_12,
        language_13,
        language_14,
        language_15,
        language_16,
        language_17
    FROM {{ ref('src_application_supported_languages')}}
)

, base AS (
{{ union_columns('languages', 'appid', ['language_1', 
                                        'language_2', 
                                        'language_3', 
                                        'language_4', 
                                        'language_5', 
                                        'language_6', 
                                        'language_7', 
                                        'language_8', 
                                        'language_9', 
                                        'language_10', 
                                        'language_11', 
                                        'language_12', 
                                        'language_13', 
                                        'language_14', 
                                        'language_15', 
                                        'language_16', 
                                        'language_17']) }} -- This macro unifies all the columns of the same type
)

SELECT DISTINCT
    appid, 
    unified_column AS supported_languages
FROM base