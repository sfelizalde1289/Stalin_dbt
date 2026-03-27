{% macro celsius_a_fahrenheit(col) %}
ROUND(({{ col }} * 9.0 / 5.0) + 32.0, 2)
{% endmacro %}
