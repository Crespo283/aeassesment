WITH base AS (
    SELECT
        Date AS price_date,
        Initialprice AS initial_price,
        Finalprice AS final_price,
        Discount AS discount,
        SPLIT(id, 'gs://ae_assesment/PriceHistory/')[1] AS appid -- The name of the field carries the path of google cloud storage
    FROM {{ ref('src_price_history') }}
)

SELECT
    price_date,
    initial_price,
    final_price,
    discount,
    SPLIT(appid, '.csv')[0] AS appid -- The name of the field still carries .csv its necessary to remove it for future relations
FROM base
ORDER BY 1, 5