-- Unifies conflict events and natural disasters into a single
-- daily disruption stream with normalized severity scoring
-- Also flags events near global shipping chokepoints

WITH conflicts AS (
    SELECT
        event_id,
        event_date AS disruption_date,
        'CONFLICT' AS disruption_category,
        event_type AS disruption_type,
        severity_level,
        ROUND(LEAST(10, GREATEST(0, severity_abs)), 2) AS severity_score,
        num_mentions AS media_intensity,
        num_articles,
        latitude,
        longitude,
        country_code,
        location_name
    FROM {{ ref('stg_conflict_events') }}
),

disasters AS (
    SELECT
        event_id,
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(pub_date AS STRING)) AS disruption_date,
        'NATURAL_DISASTER' AS disruption_category,
        event_type AS disruption_type,
        alert_level AS severity_level,
        ROUND(LEAST(10, alert_severity_score * 3.33), 2) AS severity_score,
        CAST(NULL AS INT64) AS media_intensity,
        CAST(NULL AS INT64) AS num_articles,
        latitude,
        longitude,
        country AS country_code,
        event_name AS location_name
    FROM {{ ref('stg_disaster_events') }}
),

unified AS (
    SELECT * FROM conflicts
    UNION ALL
    SELECT * FROM disasters
),

with_chokepoint_flags AS (
    SELECT
        u.*,
        c.chokepoint_name AS near_chokepoint,
        CASE
            WHEN c.chokepoint_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_near_chokepoint
    FROM unified u
    LEFT JOIN {{ ref('chokepoints') }} c
        ON u.latitude IS NOT NULL
        AND c.latitude IS NOT NULL
        AND {{ haversine_distance('u.latitude', 'u.longitude', 'c.latitude', 'c.longitude') }} < 500
)

SELECT
    event_id,
    disruption_date,
    disruption_category,
    disruption_type,
    severity_level,
    severity_score,
    CASE
        WHEN is_near_chokepoint THEN ROUND(LEAST(10, severity_score * 1.5), 2)
        ELSE severity_score
    END AS adjusted_severity_score,
    media_intensity,
    num_articles,
    latitude,
    longitude,
    country_code,
    location_name,
    near_chokepoint,
    is_near_chokepoint
FROM with_chokepoint_flags
WHERE disruption_date IS NOT NULL