SELECT DISTINCT
    player_time,
    player_count,
    appid,
    initial_price,
    final_price,
    discount
FROM {{ ref('int_player_count_history') }}