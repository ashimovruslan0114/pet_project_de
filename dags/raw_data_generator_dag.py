from datetime import datetime
import subprocess
import os

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_generator():
    """
    Запускаем наш генератор внутри контейнера Airflow
    """
    script_path = "/opt/airflow/scripts/generator/data_generator.py"

    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Generator not found: {script_path}")

    subprocess.run(["python", script_path], check=True)


with DAG(
    dag_id="raw_data_generator",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["raw", "generator"],
) as dag:

    generate_raw = PythonOperator(
        task_id="generate_raw_data",
        python_callable=run_generator,
    )
