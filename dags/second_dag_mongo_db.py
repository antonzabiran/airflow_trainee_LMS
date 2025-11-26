import os
import json
from airflow import DAG
from datetime import datetime, timedelta
# Импорт хука
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.datasets import Dataset
from airflow.providers.standard.operators.python import PythonOperator

# Созданный ранее dataset (путь должен совпадать с первым DAG!)
FINAL_FILE = "/opt/airflow/data/processed/final.json"
my_ds = Dataset(f"file://{FINAL_FILE}")


def load_to_mongo(**kwargs):
    # 1. Проверка файла
    if not os.path.exists(FINAL_FILE):
        print(f"Файла по пути нет: {FINAL_FILE}")
        return

    # 2. Чтение
    with open(FINAL_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not data:
        print("Файл пустой, нечего грузить.")
        return

    print(f'Читаем данные: {len(data)} записей')

    # 3. Подключение (ИСПРАВЛЕННЫЙ БЛОК)
    try:
        # Создаем объект Хука, указывая ID соединения
        hook = MongoHook(conn_id='mongo_default')
        # Получаем готового клиента Pymongo
        client = hook.get_conn()

        # 4. Выбор Базы и Коллекции
        db = client.airflow_db
        collection = db.comments

        # 5. Вставка
        collection.insert_many(data)
        print("Done! Данные успешно загружены в MongoDB.")

    except Exception as e:
        print(f"Ошибка при работе с MongoDB: {e}")
        raise e


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