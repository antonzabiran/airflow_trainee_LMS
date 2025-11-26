import os
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.datasets import Dataset
from airflow.providers.standard.operators.python import PythonOperator

# Созданный ранее dataset
FINAL_FILE = "/opt/airflow/data/processed/final.json"
my_ds = Dataset(f"file://{FINAL_FILE}")


def load_to_mongo(**kwargs):
    #  Проверка файла
    if not os.path.exists(FINAL_FILE):
        print(f"Файла по пути нет: {FINAL_FILE}")
        return

    # Чтение
    with open(FINAL_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Подключение
    hook = MongoHook(conn_id='mongo_default')
    client = hook.get_conn()

    db = client.airflow_db
    collection = db.comments

    collection.insert_many(data)
    print("Done! Данные успешно загружены.")



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

with DAG(
        dag_id='second_dag_mongo_db',
        default_args=default_args,
        schedule=[my_ds],  # Слушаем датасет
        start_date=datetime(2025, 1, 1),
        description='Даг для выгрузки данных в MongoDB',
        catchup=False,
) as dag:
    load_task = PythonOperator(
        task_id='load_to_mongo',
        python_callable=load_to_mongo,
    )

    load_task