-- ACTIVE RIPPLE CHAINS
-- Links disruption events to their price impacts

{{
  config(
    materialized='table'
  )
}}

WITH chains AS (
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

        affected_commodity_name,
        series_name,
        price_value,
        price_z_score,
        daily_change_pct,
        price_anomaly_level,
        actual_lag_days,
        expected_lag_days,
        chain_confidence,
        ripple_score,

        ROW_NUMBER() OVER (
            PARTITION BY event_id, affected_commodity_name
            ORDER BY ABS(price_z_score) DESC
        ) AS rn

    FROM {{ ref('int_ripple_chain_candidates') }}
    WHERE chain_confidence IN ('HIGH', 'MEDIUM', 'LOW')
),

deduplicated AS (
    SELECT * FROM chains WHERE rn = 1
)

SELECT
    CONCAT('RC-', FORMAT_DATE('%Y%m%d', disruption_date), '-', event_id) AS chain_id,
    disruption_date,
    disruption_category,
    disruption_type,
    country_code,
    location_name,
    is_near_chokepoint,
    near_chokepoint,
    adjusted_severity_score,

    affected_commodity_name,
    series_name,
    price_value,
    ROUND(price_z_score, 2) AS price_z_score,
    ROUND(daily_change_pct, 2) AS price_change_pct,
    actual_lag_days,
    expected_lag_days,
    chain_confidence,
    ROUND(ripple_score, 2) AS ripple_score,

    CURRENT_TIMESTAMP() AS calculated_at

FROM deduplicated
ORDER BY ripple_score DESC