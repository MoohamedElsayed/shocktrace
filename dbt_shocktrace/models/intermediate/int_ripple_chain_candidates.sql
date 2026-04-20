-- THE KEY MODEL: Links disruption events to price movements
-- Uses the commodity_event_mapping seed to find cause 
-- Checks if prices moved abnormally within the expected lag window

WITH events AS (
    SELECT
        event_id,
        disruption_date,
        disruption_category,
        disruption_type,
        adjusted_severity_score,
        country_code,
        location_name,
        is_near_chokepoint,
        near_chokepoint
    FROM {{ ref('int_daily_disruption_events') }}
),

price_anomalies AS (
    SELECT
        price_date,
        series_id,
        series_name,
        price_value,
        price_category,
        z_score,
        daily_change_pct,
        anomaly_level,
        is_anomaly
    FROM {{ ref('int_price_anomaly_detection') }}
),

mapping AS (
    SELECT
        country_code,
        disruption_category,
        affected_price_category,
        affected_commodity_name,
        expected_lag_days,
        impact_direction
    FROM {{ ref('commodity_event_mapping') }}
),

ripple_candidates AS (
    SELECT
        e.event_id,
        e.disruption_date,
        e.disruption_category,
        e.disruption_type,
        e.adjusted_severity_score,
        e.country_code,
        e.location_name,
        e.is_near_chokepoint,
        e.near_chokepoint,

        m.affected_price_category,
        m.affected_commodity_name,
        m.expected_lag_days,
        m.impact_direction,

        p.price_date,
        p.series_id,
        p.series_name,
        p.price_value,
        p.z_score AS price_z_score,
        p.daily_change_pct,
        p.anomaly_level AS price_anomaly_level,
        p.is_anomaly AS price_is_anomaly,

        DATE_DIFF(p.price_date, e.disruption_date, DAY) AS actual_lag_days

    FROM events e
    INNER JOIN mapping m
        ON e.country_code = m.country_code
        AND e.disruption_category = m.disruption_category
    INNER JOIN price_anomalies p
        ON p.price_category = m.affected_price_category
        AND p.price_date BETWEEN
            DATE_ADD(e.disruption_date, INTERVAL 0 DAY)
            AND DATE_ADD(e.disruption_date, INTERVAL m.expected_lag_days * 2 DAY)
)

SELECT
    event_id,
    disruption_date,
    disruption_category,
    disruption_type,
    adjusted_severity_score,
    country_code,
    location_name,
    is_near_chokepoint,
    near_chokepoint,

    affected_price_category,
    affected_commodity_name,
    expected_lag_days,
    impact_direction,

    price_date,
    series_id,
    series_name,
    price_value,
    price_z_score,
    daily_change_pct,
    price_anomaly_level,
    price_is_anomaly,
    actual_lag_days,

    CASE
        WHEN price_is_anomaly AND actual_lag_days <= expected_lag_days THEN 'HIGH'
        WHEN price_is_anomaly AND actual_lag_days <= expected_lag_days * 2 THEN 'MEDIUM'
        WHEN ABS(price_z_score) > 1 THEN 'LOW'
        ELSE 'SPECULATIVE'
    END AS chain_confidence,

    ROUND(
        adjusted_severity_score * GREATEST(1, ABS(COALESCE(price_z_score, 0))),
    2) AS ripple_score

FROM ripple_candidates