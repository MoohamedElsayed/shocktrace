-- LOOKER STUDIO: Country Risk Heatmap
-- Country vulnerability with geo data for choropleth map

{{
  config(
    materialized='table'
  )
}}

SELECT
    v.country_code,
    v.country_name,
    v.region,
    v.subregion,
    v.population,
    v.gdp_billions_usd,

    v.oil_import_dependency,
    v.food_import_dependency,
    v.trade_openness_score,
    v.geographic_risk_score,
    v.static_vulnerability_score,

    v.recent_disruptions_7d,
    v.disruption_severity_7d,
    v.chokepoint_events_7d,
    v.live_vulnerability_score,
    v.risk_level,

    r.latitude,
    r.longitude

FROM {{ ref('mart_country_vulnerability_score') }} v
LEFT JOIN {{ ref('stg_country_reference') }} r
    ON v.country_code = r.country_code