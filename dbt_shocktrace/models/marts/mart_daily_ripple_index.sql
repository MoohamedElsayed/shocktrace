-- THE DAILY RIPPLE INDEX
-- A single 0-100 score measuring global economic disruption

{{
  config(
    materialized='table',
    partition_by={
      "field": "index_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH conflict AS (
    SELECT
        disruption_date AS index_date,
        COUNT(*) AS event_count,
        SUM(adjusted_severity_score) AS total_severity,
        LEAST(100, SUM(adjusted_severity_score) * 2.5) AS conflict_sub_index
    FROM {{ ref('int_daily_disruption_events') }}
    WHERE disruption_category = 'CONFLICT'
    GROUP BY 1
),

disaster AS (
    SELECT
        disruption_date AS index_date,
        COUNT(*) AS disaster_count,
        SUM(adjusted_severity_score) AS total_severity,
        LEAST(100, SUM(adjusted_severity_score) * 10) AS disaster_sub_index
    FROM {{ ref('int_daily_disruption_events') }}
    WHERE disruption_category = 'NATURAL_DISASTER'
    GROUP BY 1
),

commodity_vol AS (
    SELECT
        price_date AS index_date,
        AVG(ABS(z_score)) AS avg_abs_z_score,
        COUNT(CASE WHEN is_anomaly THEN 1 END) AS anomaly_count,
        LEAST(100, AVG(ABS(COALESCE(z_score, 0))) * 33) AS commodity_sub_index
    FROM {{ ref('int_price_anomaly_detection') }}
    WHERE price_type = 'COMMODITY'
    GROUP BY 1
),

currency AS (
    SELECT
        rate_date AS index_date,
        AVG(COALESCE(daily_volatility_pct, 0)) AS avg_volatility,
        LEAST(100, AVG(COALESCE(daily_volatility_pct, 0)) * 50) AS currency_sub_index
    FROM {{ ref('int_currency_stress') }}
    GROUP BY 1
),

climate AS (
    SELECT
        observation_date AS index_date,
        AVG(stress_score) AS avg_stress,
        LEAST(100, AVG(stress_score) * 15) AS climate_sub_index
    FROM {{ ref('int_climate_agricultural_stress') }}
    GROUP BY 1
),

all_dates AS (
    SELECT DISTINCT index_date FROM conflict
    UNION DISTINCT
    SELECT DISTINCT index_date FROM disaster
    UNION DISTINCT
    SELECT DISTINCT index_date FROM commodity_vol
    UNION DISTINCT
    SELECT DISTINCT index_date FROM currency
    UNION DISTINCT
    SELECT DISTINCT index_date FROM climate
),

combined AS (
    SELECT
        d.index_date,

        COALESCE(co.conflict_sub_index, 0) AS conflict_score,
        COALESCE(di.disaster_sub_index, 0) AS disaster_score,
        COALESCE(cm.commodity_sub_index, 0) AS commodity_score,
        COALESCE(cu.currency_sub_index, 0) AS currency_score,
        COALESCE(cl.climate_sub_index, 0) AS climate_score,

        COALESCE(co.event_count, 0) AS conflict_events,
        COALESCE(di.disaster_count, 0) AS active_disasters,
        COALESCE(cm.anomaly_count, 0) AS price_anomalies

    FROM all_dates d
    LEFT JOIN conflict co ON d.index_date = co.index_date
    LEFT JOIN disaster di ON d.index_date = di.index_date
    LEFT JOIN commodity_vol cm ON d.index_date = cm.index_date
    LEFT JOIN currency cu ON d.index_date = cu.index_date
    LEFT JOIN climate cl ON d.index_date = cl.index_date
)

SELECT
    index_date,

    conflict_score,
    disaster_score,
    commodity_score,
    currency_score,
    climate_score,

    ROUND(
        (conflict_score   * 0.25) +
        (commodity_score  * 0.25) +
        (currency_score   * 0.20) +
        (climate_score    * 0.15) +
        (disaster_score   * 0.15)
    , 1) AS ripple_index,

    {{ classify_severity(
        '(conflict_score * 0.25) + (commodity_score * 0.25) + (currency_score * 0.20) + (climate_score * 0.15) + (disaster_score * 0.15)'
    ) }} AS risk_level,

    ROUND(
        (conflict_score * 0.25 + commodity_score * 0.25 +
         currency_score * 0.20 + climate_score * 0.15 +
         disaster_score * 0.15)
        -
        AVG(conflict_score * 0.25 + commodity_score * 0.25 +
            currency_score * 0.20 + climate_score * 0.15 +
            disaster_score * 0.15)
        OVER (ORDER BY index_date
              ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
    , 1) AS trend_vs_7d_avg,

    conflict_events,
    active_disasters,
    price_anomalies,

    CURRENT_TIMESTAMP() AS calculated_at

FROM combined
WHERE index_date IS NOT NULL