{% macro classify_severity(score_column) %}
    CASE
        WHEN {{ score_column }} >= 75 THEN 'SEVERE'
        WHEN {{ score_column }} >= 50 THEN 'HIGH'
        WHEN {{ score_column }} >= 25 THEN 'ELEVATED'
        ELSE 'LOW'
    END
{% endmacro %}