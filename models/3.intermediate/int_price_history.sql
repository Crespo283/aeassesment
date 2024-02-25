SELECT DISTINCT
    price_date,
    initial_price,
    final_price,
    discount,
    appid
FROM {{ ref('stg_price_history') }}
WHERE appid IS NOT NULL
    AND final_price IS NOT NULL
    AND initial_price IS NOT NULL
    AND discount IS NOT NULL -- The NULL data is irrelevant
GROUP BY 1, 2, 3, 4 , 5
ORDER BY appid, price_date
