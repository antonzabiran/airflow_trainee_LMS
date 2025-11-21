import os
import pandas as pd
import MongoClient
from airflow import DAG 
from airflow.sensors.filesystem import FileSensor
from airflow.operator.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

def process_file():
    folder = "/opt/airflow/data/incoming"
    files = os.listdir(folder)git init

    csv_file = None
    for filename in files:
        if filename.endswith(".csv"):
            csv_file = filename
            break

    if csv_file is None:
        print("CSV файл не найден")
        return "no file"

    print(f"Нашёл файл: {csv_file}")
    print("Начинаю обработку...")
    print("Обработка завершена")
    return "done"

with DAG(
    dag_id = "process_airflow_data", 
    default_args = default_args, 
    schedule_interval = "@daily",
    catchup = False
) as dag:

    wait_for_file = FileSensor(
                task_id = "wait_for_file",
                filepath="/opt/airflow/data/incoming",
                poke_interval = 30,
                timeout = 60 *5,
                mode = "poke"
)

