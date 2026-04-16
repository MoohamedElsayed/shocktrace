-- Cleans and standardizes commodity prices and exchange rates
-- Sources: FRED API + Frankfurter API

WITH raw_data AS (
    SELECT * FROM {{ source('raw', 'raw_commodity_prices') }}
),

cleaned AS (
    SELECT
        price_date,
        series_id,
        series_name,
        price_value,
        source AS data_source,
        
        CASE
            WHEN series_name LIKE '%crude%' OR series_name LIKE '%oil%' THEN 'OIL'
            WHEN series_name LIKE '%gas%' THEN 'GAS'
            WHEN series_name LIKE '%usd_to%' THEN 'CURRENCY'
            ELSE 'OTHER'
        END AS price_category,
        
        CASE
            WHEN source = 'FRANKFURTER' THEN 'CURRENCY'
            ELSE 'COMMODITY'
        END AS price_type,
        
        ingested_at,
        pipeline_date

    FROM raw_data
    WHERE price_value IS NOT NULL
        AND price_date IS NOT NULL
)

SELECT * FROM cleaned