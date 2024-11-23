from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Función para extraer datos
def extract_data(**kwargs):
    data = {
        "Name": ["Alice", "Bob", "Charlie", "David"],
        "Age": [24, 30, 35, 40],
        "City": ["New York", "Los Angeles", "Chicago", "Houston"],
    }
    df = pd.DataFrame(data)
    df.to_csv('/tmp/extracted_data.csv', index=False)
    print("Datos extraídos y guardados en /tmp/extracted_data.csv")

# Función para transformar datos
def transform_data(**kwargs):
    df = pd.read_csv('/tmp/extracted_data.csv')
    df = df[df['Age'] > 30]  # Filtrar personas mayores de 30 años
    df.to_csv('/tmp/transformed_data.csv', index=False)
    print("Datos transformados y guardados en /tmp/transformed_data.csv")

# Función para cargar datos
def load_data(**kwargs):
    df = pd.read_csv('/tmp/transformed_data.csv')
    print(f"Datos cargados:\n{df}")


# Definir el DAG
with DAG(
    'example_dag_every_5_minutes',  # El nombre del DAG
    default_args=default_args,
    description='Un DAG que se ejecuta cada 5 minutos',
    schedule_interval='*/5 * * * *',  # Aquí va la expresión cron
    start_date=datetime(2023, 1, 1),
    catchup=False,  # Para evitar que se ejecute para fechas pasadas
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
    )

    # Definir dependencias
    extract_task >> transform_task >> load_task
