-- Cleans and standardizes weather observations
-- Source: Open-Meteo archive API

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_weather_data') }}
),

cleaned AS (
    SELECT
        region_id,
        region_name,
        commodities_affected,
        latitude,
        longitude,
        observation_date,
        
        temp_max_c,
        temp_min_c,
        temp_mean_c,
        (temp_max_c - temp_min_c) AS temp_range_c,
        
        precipitation_mm,
        CASE
            WHEN precipitation_mm = 0 THEN 'DRY'
            WHEN precipitation_mm < 5 THEN 'LIGHT'
            WHEN precipitation_mm < 20 THEN 'MODERATE'
            ELSE 'HEAVY'
        END AS precipitation_level,
        
        wind_max_kmh,
        CASE
            WHEN wind_max_kmh > 90 THEN 'EXTREME'
            WHEN wind_max_kmh > 60 THEN 'HIGH'
            WHEN wind_max_kmh > 30 THEN 'MODERATE'
            ELSE 'LOW'
        END AS wind_severity,
        
        ingested_at,
        pipeline_date

    FROM source
    WHERE observation_date IS NOT NULL
)

SELECT * FROM cleaned