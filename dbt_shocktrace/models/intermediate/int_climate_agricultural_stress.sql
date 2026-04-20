-- Identifies weather stress in key agricultural regions
-- Maps extreme temperatures and precipitation to commodity risk

WITH weather_with_baselines AS (
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
        temp_range_c,
        precipitation_mm,
        precipitation_level,
        wind_max_kmh,
        wind_severity,

        {{ rolling_average('temp_mean_c', 'region_id', 'observation_date', 29) }}
            AS temp_avg_30d,

        {{ rolling_average('precipitation_mm', 'region_id', 'observation_date', 29) }}
            AS precip_avg_30d,

        {{ z_score('temp_mean_c', 'region_id', 'observation_date', 29) }}
            AS temp_z_score,

        {{ z_score('precipitation_mm', 'region_id', 'observation_date', 29) }}
            AS precip_z_score

    FROM {{ ref('stg_weather_data') }}
),

stress_scored AS (
    SELECT
        *,

        -- Temperature stress: too hot OR too cold
        CASE
            WHEN ABS(temp_z_score) > 2 THEN 3
            WHEN ABS(temp_z_score) > 1 THEN 2
            ELSE 1
        END AS temp_stress_score,

        CASE
            WHEN precip_z_score < -2 THEN 3     
            WHEN precip_z_score > 2 THEN 3      
            WHEN ABS(precip_z_score) > 1 THEN 2
            ELSE 1
        END AS precip_stress_score,

        CASE
            WHEN wind_max_kmh > 90 THEN 3
            WHEN wind_max_kmh > 60 THEN 2
            ELSE 1
        END AS wind_stress_score

    FROM weather_with_baselines
)

SELECT
    region_id,
    region_name,
    commodities_affected,
    latitude,
    longitude,
    observation_date,
    temp_mean_c,
    temp_avg_30d,
    temp_z_score,
    precipitation_mm,
    precip_avg_30d,
    precip_z_score,
    wind_max_kmh,

    temp_stress_score,
    precip_stress_score,
    wind_stress_score,

    ROUND(
        LEAST(10, (temp_stress_score + precip_stress_score + wind_stress_score) * 1.11),
    2) AS stress_score,

    CASE
        WHEN (temp_stress_score + precip_stress_score + wind_stress_score) >= 8 THEN 'EXTREME'
        WHEN (temp_stress_score + precip_stress_score + wind_stress_score) >= 6 THEN 'HIGH'
        WHEN (temp_stress_score + precip_stress_score + wind_stress_score) >= 4 THEN 'MODERATE'
        ELSE 'LOW'
    END AS stress_level

FROM stress_scored
WHERE observation_date IS NOT NULL