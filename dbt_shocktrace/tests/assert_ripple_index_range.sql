-- Ripple index must always be between 0 and 100
SELECT index_date, ripple_index
FROM {{ ref('mart_daily_ripple_index') }}
WHERE ripple_index < 0 OR ripple_index > 100