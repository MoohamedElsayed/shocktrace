-- Cleans and standardizes GDELT conflict events
-- Source: raw.raw_gdelt_events (GDELT public BigQuery dataset)

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_gdelt_events') }}
),

cleaned AS (
    SELECT
        CAST(global_event_id AS STRING) AS event_id,
        event_date,
        
        event_code,
        event_root_code,
        event_type,
        
        UPPER(TRIM(actor1_country)) AS actor1_country,
        INITCAP(actor1_name) AS actor1_name,
        UPPER(TRIM(actor2_country)) AS actor2_country,
        INITCAP(actor2_name) AS actor2_name,
        
        goldstein_scale,
        ABS(goldstein_scale) AS severity_abs,
        CASE
            WHEN goldstein_scale <= -8 THEN 'EXTREME'
            WHEN goldstein_scale <= -5 THEN 'HIGH'
            WHEN goldstein_scale <= -2 THEN 'MODERATE'
            ELSE 'LOW'
        END AS severity_level,
        
        num_mentions,
        num_sources,
        num_articles,
        avg_tone,
        
        action_lat AS latitude,
        action_lon AS longitude,
        UPPER(TRIM(action_country)) AS country_code,
        action_location_name AS location_name,
        
        source_url,
        
        ingested_at,
        pipeline_date

    FROM source
    WHERE event_date IS NOT NULL
        AND event_type IS NOT NULL
)

SELECT * FROM cleaned