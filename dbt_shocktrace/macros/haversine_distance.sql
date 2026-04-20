{% macro haversine_distance(lat1, lon1, lat2, lon2) %}
    (6371 * ACOS(
        LEAST(1.0, GREATEST(-1.0,
            COS(ACOS(-1) * {{ lat1 }} / 180) * COS(ACOS(-1) * {{ lat2 }} / 180)
            * COS(ACOS(-1) * ({{ lon2 }} - {{ lon1 }}) / 180)
            + SIN(ACOS(-1) * {{ lat1 }} / 180) * SIN(ACOS(-1) * {{ lat2 }} / 180)
        ))
    ))
{% endmacro %}