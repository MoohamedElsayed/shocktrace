-- Cleans and standardizes disaster events
-- Sources: GDACS RSS + USGS earthquake API

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_disaster_events') }}
),

cleaned AS (
    SELECT
        event_id,
        UPPER(TRIM(event_type)) AS event_type,
        UPPER(TRIM(alert_level)) AS alert_level,
        
        CASE UPPER(TRIM(alert_level))
            WHEN 'RED' THEN 3
            WHEN 'ORANGE' THEN 2
            WHEN 'GREEN' THEN 1
            ELSE 0
        END AS alert_severity_score,
        
        severity_value,
        event_name,
        description,
        pub_date,
        
        latitude,
        longitude,
        country,
        
        source,
        
        ingested_at,
        pipeline_date

    FROM source
    WHERE event_id IS NOT NULL
)

SELECT * FROM cleaned