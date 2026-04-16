-- Cleans and standardizes country reference data
-- Sources: REST Countries API + World Bank API

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_country_reference') }}
),

cleaned AS (
    SELECT
        country_code,
        country_name,
        population,
        region,
        subregion,
        borders,
        latitude,
        longitude,
        
        gdp_usd,
        ROUND(gdp_usd / 1e9, 2) AS gdp_billions_usd,
        trade_pct_of_gdp,
        imports_pct_of_gdp,
        
        CASE
            WHEN trade_pct_of_gdp > 100 THEN 'EXTREME'
            WHEN trade_pct_of_gdp > 60 THEN 'HIGH'
            WHEN trade_pct_of_gdp > 30 THEN 'MODERATE'
            ELSE 'LOW'
        END AS trade_vulnerability,
        
        ingested_at,
        pipeline_date

    FROM source
    WHERE country_code IS NOT NULL
)

SELECT * FROM cleaned
