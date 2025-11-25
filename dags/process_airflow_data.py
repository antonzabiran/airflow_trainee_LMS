import os
from io import StringIO
import pandas as pd

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

# настройки даг 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# ищет CSV в папке
def process_file_func(**kwargs):
    file_path = "/opt/airflow/data/incoming/example_input.csv"
    print("файл найдет :", file_path)
    kwargs['ti'].xcom_push(key='csv_file_arg', value=file_path)
    return file_path

# читает CSV и выводит датафрейм
def file_read_csv_func(**kwargs):
    ti = kwargs["ti"]
    file_path = ti.xcom_pull(task_ids="process_file_task", key='csv_file_arg')

    print("Читаем файл:", file_path)

    if not file_path:
        raise ValueError("Путь к CSV НЕ получен из XCom!")

    df = pd.read_csv(file_path)
    json_str = df.to_json(orient = 'records')
    ti.xcom_push(key="df_raw", value=json_str)


def branch_logic(**kwargs):
    ti = kwargs["ti"]
    json_str = ti.xcom_pull(task_ids="file_read_csv", key="df_raw")
    df = pd.read_json(json_str)
    print("Проверяю пустой ли DF:", df)
    if df.empty:
        print("Файл пустой")
        return "log_empty_file"
    else:
        print("Файл не пустой")
        return "process_task.replace_null"

def replace_nulls_func(**kwargs):
    file_path = kwargs['ti'].xcom_pull(
        task_ids='process_file_task',
        key='csv_file_arg'
    )
    print(f'ya tut {file_path}')
    df = pd.read_csv(file_path)

    df["content"] = df["content"].replace(["null", ""], "-")
    df["content"] = df["content"].fillna("-")

    json_str = df.to_json(orient="records")
    kwargs['ti'].xcom_push(key="not_null", value=json_str)

# сортировка по дате создания 
def sort_data_by_created_date(**kwargs):
    ti = kwargs['ti']
    json_str = ti.xcom_pull(
        task_ids='process_task.replace_null',
        key='not_null'
    )
    print("я тут")

    df = pd.read_json(StringIO(json_str))
    print("я тут2")

    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")

    df = df.sort_values(by="created_date")

    to_json = df.to_json(orient="records")
    ti.xcom_push(key='sorted_date', value=to_json)


# убираем лишние символы и смайлики 
def clear_text(**kwargs):
    json_str = kwargs['ti'].xcom_pull(
        task_ids = 'process_task.sort_data',
        key='sorted_date')
    df = pd.read_json(StringIO(json_str))
    print(f'мы здесь')
    df["content"] = df["content"].str.replace(
        r"[^A-Za-zА-Яа-я0-9 .,!?-]", "", regex=True
    )

    to_json_final = df.to_json(orient='records')
    kwargs['ti'].xcom_push(key='clean_content', value=to_json_final)


with DAG(
    dag_id='waiting_file_data',
    default_args=default_args,
    description="This DAG waiting file",
    schedule='@daily',
    catchup=False
) as dag:

    # ждет появления файла 
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/opt/airflow/data/incoming/example_input.csv',
        poke_interval=10,
        timeout=60 * 5
    )

    # ищет CSV файл и передает его в икском 
    process_file_task = PythonOperator(
        task_id='process_file_task',
        python_callable=process_file_func
    )

    # читает CSV и возвращает DataFrame 
    file_read_csv_task = PythonOperator(
        task_id='file_read_csv',
        python_callable=file_read_csv_func
    )

    # branch 
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_logic
    )

    # если файл пустой , то просто логируем 
    log_empty_file = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "file is empty"'
    )

    with TaskGroup(group_id='process_task') as process_group:
        task_1 = PythonOperator(
            task_id='replace_null',
            python_callable=replace_nulls_func
        )

        task_2 = PythonOperator(
            task_id='sort_data',
            python_callable=sort_data_by_created_date
        )

        task_3 = PythonOperator(
            task_id='clear_text',
            python_callable=clear_text
        )

        task_1 >> task_2 >> task_3

    wait_for_file >> process_file_task >> file_read_csv_task >> branch_task
    branch_task >> log_empty_file
    branch_task >> process_group