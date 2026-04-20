-- COMMODITY FORECAST SIGNALS
-- Leading indicators: which commodities might spike next
-- Based on active disruptions + weather + mapping

{{
  config(
    materialized='table'
  )
}}

WITH active_events AS (
    SELECT
        country_code,
        disruption_category,
        COUNT(*) AS event_count,
        SUM(adjusted_severity_score) AS total_severity,
        MAX(disruption_date) AS latest_event_date,
        COUNTIF(is_near_chokepoint) AS chokepoint_events
    FROM {{ ref('int_daily_disruption_events') }}
    WHERE disruption_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY 1, 2
),

-- What commodities should be affected by current events
expected_impacts AS (
    SELECT
        m.affected_price_category,
        m.affected_commodity_name,
        m.impact_direction,
        m.expected_lag_days,
        e.country_code,
        e.disruption_category,
        e.event_count,
        e.total_severity,
        e.latest_event_date,
        e.chokepoint_events
    FROM active_events e
    INNER JOIN {{ ref('commodity_event_mapping') }} m
        ON e.country_code = m.country_code
        AND e.disruption_category = m.disruption_category
),

-- Current price status
latest_prices AS (
    SELECT
        series_id,
        series_name,
        price_category,
        price_value,
        price_date,
        z_score,
        daily_change_pct,
        anomaly_level,
        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY price_date DESC) AS rn
    FROM {{ ref('int_price_anomaly_detection') }}
),

current_prices AS (
    SELECT * FROM latest_prices WHERE rn = 1
),

climate_signals AS (
    SELECT
        commodities_affected,
        AVG(stress_score) AS avg_climate_stress,
        MAX(stress_level) AS max_stress_level
    FROM {{ ref('int_climate_agricultural_stress') }}
    WHERE observation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY 1
)

SELECT
    ei.affected_commodity_name AS commodity,
    ei.affected_price_category AS price_category,
    ei.impact_direction AS expected_direction,

    SUM(ei.event_count) AS triggering_events,
    SUM(ei.total_severity) AS total_disruption_severity,
    MAX(ei.latest_event_date) AS latest_trigger_date,
    SUM(ei.chokepoint_events) AS chokepoint_events,
    MIN(ei.expected_lag_days) AS earliest_expected_impact_days,

    MAX(cp.price_value) AS current_price,
    MAX(cp.z_score) AS current_z_score,
    MAX(cp.anomaly_level) AS current_anomaly_level,

    ROUND(LEAST(100,
        SUM(ei.total_severity) * 5 +
        SUM(ei.chokepoint_events) * 10
    ), 1) AS signal_strength,

    {{ classify_severity(
        'LEAST(100, SUM(ei.total_severity) * 5 + SUM(ei.chokepoint_events) * 10)'
    ) }} AS signal_level,

    CURRENT_TIMESTAMP() AS calculated_at

FROM expected_impacts ei
LEFT JOIN current_prices cp
    ON cp.price_category = ei.affected_price_category
GROUP BY 1, 2, 3
ORDER BY signal_strength DESC