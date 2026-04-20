{% macro z_score(value_column, partition_by, order_by, lookback_rows=29) %}
    SAFE_DIVIDE(
        ({{ value_column }}) - AVG({{ value_column }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ lookback_rows }} PRECEDING AND 1 PRECEDING
        ),
        STDDEV({{ value_column }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ lookback_rows }} PRECEDING AND 1 PRECEDING
        )
    )
{% endmacro %}