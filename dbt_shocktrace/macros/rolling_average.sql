{% macro rolling_average(value_column, partition_by, order_by, lookback_rows=29) %}
    AVG({{ value_column }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ lookback_rows }} PRECEDING AND 1 PRECEDING
    )
{% endmacro %}