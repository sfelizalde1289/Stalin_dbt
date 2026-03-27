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
    dag_id='dag_dbt_calidad_aire',
    description='Carga y transformación de datos de calidad del aire con dbt',
    schedule_interval=None, 
    start_date=datetime(2026, 2, 10),
    catchup=False, #tarea_calidad_aire
    tags=['ETL', 'dbt', 'Jinja', 'UNL']
)
def flujo_calidad_aire():
    
    DBT_DIR = "/opt/airflow/dbt/proyecto_stalin"

    # TAREA 1: Crear seed de calidad del aire
    @task
    def crear_seed_calidad_aire():
        seeds_dir = os.path.join(DBT_DIR, "seeds")
        os.makedirs(seeds_dir, exist_ok=True)
        with open(os.path.join(seeds_dir, "calidad_aire.csv"), "w") as f:
            f.write("""ciudad_id,indice_ica,particulas_pm25
Loja,45,12.5
Quito,110,35.2
Guayaquil,85,25.0
Cuenca,55,15.1""")
        print("Seed 'calidad_aire.csv' creado correctamente.")

    # TAREA 2: Sembrar seed en la base de datos
    @task
    def sembrar_calidad_aire():
        ejecutar_dbt_resiliente("dbt seed", DBT_DIR)

    # TAREA 3: Crear staging para calidad del aire
    @task
    def crear_staging_calidad_aire():
        ejecutar_dbt_resiliente("dbt run --models staging.*", DBT_DIR)

    # TAREA 4: Ejecutar modelo final (Marts)
    @task
    def ejecutar_staging_calidad_aire():
        ejecutar_dbt_resiliente("dbt run --models marts.*", DBT_DIR)

    # ==========================================
    # 3. LINAJE DE TAREAS
    # ==========================================
    crear_seed_calidad_aire() >> sembrar_calidad_aire() >> crear_staging_calidad_aire() >> ejecutar_staging_calidad_aire()

# Instanciar el DAG
dag_principal = flujo_calidad_aire()