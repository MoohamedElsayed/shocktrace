-- LOOKER STUDIO: Disruption Map Data
-- Geo-located events for map visualization

{{
  config(
    materialized='table'
  )
}}

SELECT
    event_id,
    disruption_date,
    disruption_category,
    disruption_type,
    severity_level,
    adjusted_severity_score,
    media_intensity,
    latitude,
    longitude,
    country_code,
    location_name,
    is_near_chokepoint,
    near_chokepoint,

    CONCAT(
        disruption_type, ' in ',
        COALESCE(location_name, country_code),
        ' (Severity: ', CAST(ROUND(adjusted_severity_score, 1) AS STRING), ')'
    ) AS event_label,

    GREATEST(1, ROUND(adjusted_severity_score * 2)) AS bubble_size

FROM {{ ref('int_daily_disruption_events') }}
WHERE latitude IS NOT NULL
    AND longitude IS NOT NULL