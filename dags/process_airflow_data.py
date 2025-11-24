import os 
import pandas as pd 

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor 
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
    folder = '/opt/airflow/data/incoming'
    files = os.listdir(folder)
    csv_file = None
    for filename in files:
        if filename.endswith(".csv"): 
            csv_file = filename
            break
    if csv_file is None:
        print("File empty")
        return "No FILE"
    print("Find file")
# полный путь к папке
    full_folder_file = os.path.join(folder, csv_file)
    print("Path to file :", full_folder_file) 
# передаем путь файла в к XCOM
    kwargs['ti'].xcom_push(key='csv_file_arg', value = full_folder_file)
    return full_folder_file

# читает CSV и выводит датафрейм
def file_read_csv_func(**kwargs):
    full_path = kwargs['ti'].xcom_pull( key = 'csv_file_arg')
    df = pd.read_csv(full_path)
    print(df)
    return df 


def branch_logic(**kwargs):
    file_path = kwargs['ti'].xcom_pull( key = 'csv_file_arg')
    df = pd.read_csv(file_path)

    if df.empty:
        print("файл пустой")
        return "log_empty_file"
    else:
        print("файл не пустой")
        return "process_task.replace_null"
    
def replace_nulls_func(**kwargs):
    file_path = kwargs['ti'].xcom_pull( key = 'csv_file_arg')
    df = pd.read_csv(file_path)

    new_df = df.replace(["null", ""], "-")
    df = new_df.fillna("-")

    json_str = df.to_json(orient = "records")
    kwargs['ti'].xcom_push(key = "not_null", value = json_str)
# сортировка по дате создания 
def sort_data_by_created_date(**kwargs):
    json_str = kwargs['ti'].xcom_pull(key = "not_null")
    df = pd.read_json(json_str)
    df["created_date"] = pd.to_datetime(df["created_date"])
    sort_values = df.sort_values(by="created_date")
    to_json = sort_values.to_json(orient = "records")
    kwargs['ti'].xcom_push(key = 'sorted_date', value = to_json)

# убираем лишние символы и смайлики 
def clear_text (**kwargs):
    json_str = kwargs['ti'].xcom_pull(key = 'sorted_date')
    df = pd.read_json(json_str)

    df["content"] = df["content"].str.replace(r"[^A-Za-zА-Яа-я0-9 .,!?-]", "", regex = True)

    to_json_final = df.to_json(orient = 'records')
    kwargs['ti'].xcom_push(key = 'clean_content', value = to_json_final)



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
        filepath='/opt/airflow/data/incoming/',
        poke_interval=30,
        timeout=60 * 5,
        mode='poke'
    )
# ищет CSV файл и передает его в икском 
    process_file_task = PythonOperator(
        task_id='process_file',
        python_callable=process_file_func
    )
# читает CSV и возвращает DataFrame 
    file_read_csv_task = PythonOperator(
        task_id='file_read_csv',
        python_callable=file_read_csv_func
    )

# branch 
    branch_task = BranchPythonOperator(
        task_id = 'branch_task',
        python_callable = branch_logic
    )
# если файл пустой , то просто логируем 
    log_empty_file = BashOperator(
        task_id = 'log_empty_file',
        bash_command = 'echo "file is empty"'
    )

    with TaskGroup (group_id = 'process_task') as process_group:
        task_1 = PythonOperator(
            task_id = 'replace_null',
            python_callable = replace_nulls_func)
        
        task_2 = PythonOperator(
            task_id = 'sort_data',
            python_callable = sort_data_by_created_date)
        
        task_3 = PythonOperator(
            task_id = 'clear_text',
            python_callable = clear_text)
        
        task_1 >> task_2 >> task_3

    wait_for_file >> process_file_task >> file_read_csv_task >> branch_task

    branch_task >> log_empty_file
    branch_task >> process_groupe