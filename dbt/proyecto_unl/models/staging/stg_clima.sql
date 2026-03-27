WITH source AS (
    SELECT * FROM {{ source('raw_data', 'datos_clima') }}
)
SELECT ciudad AS nombre_ciudad, temperatura AS temp_celsius, clima AS condicion_climatica FROM source
