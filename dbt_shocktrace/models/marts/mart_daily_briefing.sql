-- DAILY BRIEFING
-- One row per day with structured summary

{{
  config(
    materialized='table',
    partition_by={
      "field": "briefing_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH ripple AS (
    SELECT
        index_date AS briefing_date,
        ripple_index,
        risk_level,
        trend_vs_7d_avg,
        conflict_score,
        commodity_score,
        currency_score,
        climate_score,
        disaster_score,
        conflict_events,
        active_disasters,
        price_anomalies
    FROM {{ ref('mart_daily_ripple_index') }}
),

-- Top disruption events per day
top_events AS (
    SELECT
        disruption_date,
        ARRAY_AGG(
            STRUCT(
                disruption_type,
                country_code,
                location_name,
                ROUND(adjusted_severity_score, 1) AS severity
            )
            ORDER BY adjusted_severity_score DESC
            LIMIT 3
        ) AS top_3_events
    FROM {{ ref('int_daily_disruption_events') }}
    GROUP BY 1
),

-- Most volatile commodity per day
top_commodity AS (
    SELECT
        price_date,
        ARRAY_AGG(
            STRUCT(
                series_name,
                ROUND(z_score, 2) AS z_score,
                ROUND(daily_change_pct, 2) AS change_pct
            )
            ORDER BY ABS(z_score) DESC
            LIMIT 1
        )[OFFSET(0)] AS most_volatile
    FROM {{ ref('int_price_anomaly_detection') }}
    WHERE price_type = 'COMMODITY'
        AND z_score IS NOT NULL
    GROUP BY 1
)

SELECT
    r.briefing_date,
    r.ripple_index,
    r.risk_level,
    r.trend_vs_7d_avg,

    CASE
        WHEN r.trend_vs_7d_avg > 5 THEN 'WORSENING'
        WHEN r.trend_vs_7d_avg < -5 THEN 'IMPROVING'
        ELSE 'STABLE'
    END AS outlook_signal,

    r.conflict_score,
    r.commodity_score,
    r.currency_score,
    r.climate_score,
    r.disaster_score,

    r.conflict_events,
    r.active_disasters,
    r.price_anomalies,

    te.top_3_events,
    tc.most_volatile AS most_volatile_commodity,

    CURRENT_TIMESTAMP() AS generated_at

FROM ripple r
LEFT JOIN top_events te ON r.briefing_date = te.disruption_date
LEFT JOIN top_commodity tc ON r.briefing_date = tc.price_date

