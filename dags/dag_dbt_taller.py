import os
import subprocess
from datetime import datetime
from airflow.decorators import dag, task
from tenacity import retry, stop_after_attempt, wait_exponential

# ==========================================
# 1. FUNCIÓN DE RESILIENCIA (TENACITY)
# ==========================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))

def ejecutar_dbt_resiliente(comando: str, directorio: str):

    print(f"Ejecutando: {comando} en {directorio}")

    resultado = subprocess.run(
        comando, cwd=directorio, shell=True, capture_output=True, text=True
    )
    if resultado.returncode != 0:
        print(f"Error dbt:\n{resultado.stderr}\n{resultado.stdout}")
        raise Exception(f"Fallo en: {comando}")
    
    print(f"Éxito:\n{resultado.stdout}")
    return True

# ==========================================
# 2. DEFINICIÓN DEL DAG (TASKFLOW API)
# ==========================================
@dag(
    dag_id='dag_dbt_taller',
    description='Generación y ejecución dinámica de dbt con Jinja y Tenacity',
    schedule_interval=None, 
    start_date=datetime(2026, 2, 10),
    catchup=False,
    tags=['ETL', 'dbt', 'Jinja', 'UNL']
)

def taller_semana10_flujo():
    
    DBT_DIR = "/opt/airflow/dbt/proyecto_stalin"

    # TAREA 1: Generar toda la estructura y código SQL con Jinja dinámicamente
    @task
    def desplegar_codigo_dbt():
        # Definir rutas
        dirs = {
            "staging": os.path.join(DBT_DIR, "models", "staging"),
            "marts": os.path.join(DBT_DIR, "models", "marts"),
            "macros": os.path.join(DBT_DIR, "macros"),
            "seeds": os.path.join(DBT_DIR, "seeds")
        }
        
        # Crear directorios si no existen
        for d in dirs.values():
            os.makedirs(d, exist_ok=True)

        # 1. Crear Semilla (CSV)
        with open(os.path.join(dirs["seeds"], "datos_clima.csv"), "w") as f:
            f.write("ciudad,temperatura,clima\nLoja,22,Soleado\nQuito,15,Nublado\nGuayaquil,30,Caluroso\nCuenca,18,Lluvioso")

        # 2. Crear sources.yml
        with open(os.path.join(dirs["staging"], "sources.yml"), "w") as f:
            f.write("version: 2\nsources:\n  - name: raw_data\n    schema: public\n    tables:\n      - name: datos_clima")

        # 3. Crear Modelo Staging (stg_clima.sql)
        with open(os.path.join(dirs["staging"], "stg_clima.sql"), "w") as f:
            f.write("""WITH source AS (
    SELECT * FROM {{ source('raw_data', 'datos_clima') }}
)
SELECT ciudad AS nombre_ciudad, temperatura AS temp_celsius, clima AS condicion_climatica FROM source
""")

        # 4. Crear Macro en Jinja (utils_temperatura.sql)
        with open(os.path.join(dirs["macros"], "utils_temperatura.sql"), "w") as f:
            f.write("""{% macro celsius_a_fahrenheit(col) %}
ROUND(({{ col }} * 9.0 / 5.0) + 32.0, 2)
{% endmacro %}
""")

        # 5. Crear Modelo Marts con Bucle FOR en Jinja (fct_resumen_clima.sql)
        with open(os.path.join(dirs["marts"], "fct_resumen_clima.sql"), "w") as f:
            f.write("""{{ config(materialized='table') }}

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
""")
        print("Archivos dbt creados exitosamente mediante Python.")

    # TAREA 2: Sembrar datos
    @task
    def sembrar_datos():
        ejecutar_dbt_resiliente("dbt seed", DBT_DIR)

    # TAREA 3: Ejecutar Transformaciones
    @task
    def ejecutar_transformaciones():
        ejecutar_dbt_resiliente("dbt run", DBT_DIR)

    # ==========================================
    # LINAJE DE TAREAS (DEPENDENCIAS)
    # ==========================================
    desplegar_codigo_dbt() >> sembrar_datos() >> ejecutar_transformaciones()

# Instanciar el DAG
dag_principal = taller_semana10_flujo()