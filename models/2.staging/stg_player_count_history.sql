WITH player_count_history_1 AS (
    SELECT
        Time AS player_time,
        playercount AS player_count,
        SPLIT(filename, 'gs://ae_assesment/PlayerCountHistoryPart1/')[1] AS appid -- The name of the field carries the path of google cloud storage
    FROM {{ ref('src_player_count_history_1') }}
)

, player_count_history_2 AS (
    SELECT
        Time AS player_time,
        playercount AS player_count,
        SPLIT(filename, 'gs://ae_assesment/PlayerCountHistoryPart2/PlayerCountHistoryPart2/')[1] AS appid -- The name of the field carries the path of google cloud storage
    FROM {{ ref('src_player_count_history_2') }}
)

, base_1 AS (
    SELECT
        DATE(player_time) AS player_time, -- Time in minutes and hours for the Player Count is irrelebant. Will be more useful as a Date
        AVG(player_count) AS player_count,
        SPLIT(appid, '.csv')[0] AS appid -- The name of the field still carries .csv its necessary to remove it for future relations
    FROM player_count_history_1
    GROUP BY 1, 3
    ORDER BY 1
)

, base_2 AS (
    SELECT
        DATE(player_time) AS player_time, -- Time in minutes and hours for the Player Count is irrelebant. Will be more useful as a Date
        AVG(player_count) AS player_count,
        SPLIT(appid, '.csv')[0] AS appid -- The name of the field still carries .csv its necessary to remove it for future relations
    FROM player_count_history_2
    GROUP BY 1, 3
    ORDER BY 1
)

SELECT
    player_time,
    player_count,
    appid
FROM base_1
UNION ALL
SELECT
    player_time,
    player_count,
    appid
FROM base_2
