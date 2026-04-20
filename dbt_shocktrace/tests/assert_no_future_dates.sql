-- No dates should be in the future
SELECT index_date
FROM {{ ref('mart_daily_ripple_index') }}
WHERE index_date > CURRENT_DATE()