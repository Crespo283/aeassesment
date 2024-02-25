SELECT DISTINCT
    pch.player_time,
    pch.player_count,
    pch.appid,
    ph.initial_price,
    ph.final_price,
    ph.discount
FROM {{ ref('stg_player_count_history') }} AS pch
LEFT JOIN {{ ref('int_price_history')}} AS ph ON pch.appid = ph.appid
    AND pch.player_time = ph.price_date
WHERE pch.appid IS NOT NULL
    AND pch.player_count IS NOT NULL -- The NULL data is irrelevant
    AND pch.player_count > 0 -- Games or apps with 0 players have no relevant data
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY pch.appid, pch.player_time
