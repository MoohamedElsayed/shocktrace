-- LOOKER STUDIO: Price Movements
-- Commodity and currency price tracking with anomaly flags

{{
  config(
    materialized='table'
  )
}}

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
    daily_change_pct,
    anomaly_level,
    is_anomaly,

    CASE
        WHEN daily_change_pct > 0 THEN 'UP'
        WHEN daily_change_pct < 0 THEN 'DOWN'
        ELSE 'FLAT'
    END AS direction,

    ABS(daily_change_pct) AS abs_change_pct

FROM {{ ref('int_price_anomaly_detection') }}
WHERE price_date IS NOT NULL