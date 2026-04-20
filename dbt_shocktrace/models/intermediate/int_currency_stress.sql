-- Identifies currencies under pressure by measuring
-- daily volatility and deviation from rolling baseline

WITH currency_data AS (
    SELECT
        price_date AS rate_date,
        series_id,
        series_name,
        price_value AS rate_value,
        price_category

    FROM {{ ref('stg_commodity_prices') }}
    WHERE price_type = 'CURRENCY'
),

with_stats AS (
    SELECT
        rate_date,
        series_id,
        series_name,
        rate_value,

        LAG(rate_value)
            OVER (PARTITION BY series_id ORDER BY rate_date)
            AS prev_rate,

        SAFE_DIVIDE(
            ABS(rate_value - LAG(rate_value)
                OVER (PARTITION BY series_id ORDER BY rate_date)),
            LAG(rate_value)
                OVER (PARTITION BY series_id ORDER BY rate_date)
        ) * 100 AS daily_volatility_pct,

        {{ z_score('rate_value', 'series_id', 'rate_date', 29) }}
            AS z_score,

        {{ rolling_average('rate_value', 'series_id', 'rate_date', 29) }}
            AS rolling_avg_30d

    FROM currency_data
)

SELECT
    rate_date,
    series_id,
    series_name,
    rate_value,
    prev_rate,
    ROUND(daily_volatility_pct, 4) AS daily_volatility_pct,
    z_score,
    rolling_avg_30d,

    CASE
        WHEN daily_volatility_pct > 2 THEN 'SEVERE'
        WHEN daily_volatility_pct > 1 THEN 'HIGH'
        WHEN daily_volatility_pct > 0.5 THEN 'ELEVATED'
        ELSE 'NORMAL'
    END AS stress_level,

    CASE
        WHEN daily_volatility_pct > 1 THEN TRUE
        ELSE FALSE
    END AS is_stressed

FROM with_stats
WHERE rate_date IS NOT NULL