-- Cleans and standardizes EIA energy data
-- Source: EIA API

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_energy_data') }}
),

cleaned AS (
    SELECT
        series_id,
        series_name,
        frequency,
        period,
        value AS metric_value,
        
        CASE
            WHEN series_name LIKE '%stocks%' THEN 'INVENTORY'
            WHEN series_name LIKE '%production%' THEN 'PRODUCTION'
            WHEN series_name LIKE '%utilization%' THEN 'UTILIZATION'
            WHEN series_name LIKE '%imports%' THEN 'IMPORTS'
            ELSE 'OTHER'
        END AS metric_category,
        
        ingested_at,
        pipeline_date

    FROM source
    WHERE value IS NOT NULL
)

SELECT * FROM cleaned