WITH tags AS (
    SELECT 
        id AS appid,
        tag_1,
        tag_2,
        tag_3,
        tag_4,
        tag_5,
        tag_6,
        tag_7,
        tag_8,
        tag_9,
        tag_10,
        tag_11,
        tag_12,
        tag_13,
        tag_14,
        tag_15,
        tag_16,
        tag_17,
        tag_18,
        tag_19
    FROM {{ ref('src_application_tags') }}
) 

, base AS(
{{ union_columns('tags', 'appid', ['tag_1', 
                                    'tag_2', 
                                    'tag_3', 
                                    'tag_4', 
                                    'tag_5', 
                                    'tag_6', 
                                    'tag_7', 
                                    'tag_8', 
                                    'tag_9', 
                                    'tag_10', 
                                    'tag_11', 
                                    'tag_12', 
                                    'tag_13', 
                                    'tag_14', 
                                    'tag_15', 
                                    'tag_16', 
                                    'tag_17', 
                                    'tag_18', 
                                    'tag_19']) }} -- This macro unifies all the columns of the same type
)

SELECT DISTINCT
    appid,
    tag
FROM base