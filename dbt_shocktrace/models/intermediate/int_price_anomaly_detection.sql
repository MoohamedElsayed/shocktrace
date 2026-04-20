-- Detects price anomalies using z-scores against 30-day rolling baseline
-- Flags commodities and currencies with unusual price movements

WITH prices_with_stats AS (
    SELECT
        price_date,
        series_id,
        series_name,
        price_value,
        price_category,
        price_type,

        {{ rolling_average('price_value', 'series_id', 'price_date', 29) }}
            AS rolling_avg_30d,

        {{ z_score('price_value', 'series_id', 'price_date', 29) }}
            AS z_score,

        price_value - LAG(price_value)
            OVER (PARTITION BY series_id ORDER BY price_date)
            AS daily_change,

        SAFE_DIVIDE(
            price_value - LAG(price_value)
                OVER (PARTITION BY series_id ORDER BY price_date),
            LAG(price_value)
                OVER (PARTITION BY series_id ORDER BY price_date)
        ) * 100 AS daily_change_pct

    FROM {{ ref('stg_commodity_prices') }}
)

SELECT
    price_date,
    series_id,
    series_name,
    price_value,
    price_category,
    price_type,
    rolling_avg_30d,
    z_score,
    daily_change,
    ROUND(daily_change_pct, 2) AS daily_change_pct,

    CASE
        WHEN ABS(z_score) > 3 THEN 'EXTREME'
        WHEN ABS(z_score) > 2 THEN 'HIGH'
        WHEN ABS(z_score) > 1 THEN 'ELEVATED'
        ELSE 'NORMAL'
    END AS anomaly_level,

    CASE
        WHEN ABS(z_score) > 2 THEN TRUE
        ELSE FALSE
    END AS is_anomaly

FROM prices_with_stats
WHERE price_date IS NOT NULL