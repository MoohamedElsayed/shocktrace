-- LOOKER STUDIO: Ripple Index Overview
-- Flat table optimized for Looker Studio dashboard

{{
  config(
    materialized='table'
  )
}}

SELECT
    index_date,
    ripple_index,
    risk_level,
    trend_vs_7d_avg,

    CASE
        WHEN trend_vs_7d_avg > 5 THEN 'WORSENING'
        WHEN trend_vs_7d_avg < -5 THEN 'IMPROVING'
        ELSE 'STABLE'
    END AS outlook,

    conflict_score,
    commodity_score,
    currency_score,
    climate_score,
    disaster_score,

    conflict_events,
    active_disasters,
    price_anomalies,

    EXTRACT(YEAR FROM index_date) AS year,
    EXTRACT(MONTH FROM index_date) AS month,
    FORMAT_DATE('%A', index_date) AS day_of_week,

    calculated_at

FROM {{ ref('mart_daily_ripple_index') }}
WHERE index_date IS NOT NULL