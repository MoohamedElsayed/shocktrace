-- Prices should never be negative
SELECT price_date, series_id, price_value
FROM {{ ref('stg_commodity_prices') }}
WHERE price_value < 0