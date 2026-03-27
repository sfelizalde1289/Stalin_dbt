{{ config(materialized='table') }}

{% set climas = ['Soleado', 'Nublado', 'Caluroso', 'Lluvia'] %}

WITH staging AS (
    SELECT * FROM {{ ref('stg_clima') }}
)

SELECT 
    nombre_ciudad,
    {{ celsius_a_fahrenheit('temp_celsius') }} AS temp_fahrenheit,
    {% for clima in climas %}
    SUM(CASE WHEN condicion_climatica = '{{ clima }}' THEN 1 ELSE 0 END) AS dias_{{ clima | lower }}
    {% if not loop.last %} , {% endif %}
    {% endfor %}
FROM staging
GROUP BY nombre_ciudad, temp_celsius
