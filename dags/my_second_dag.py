from airflow.models import DAG
from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.filesystem import FileSensor


with DAG('dag_sensor', schedule_interval = '@daily',start_date = datetime(2025,1,1),
        default_args = default_args, catchup = False ) as dag:
    
            waiting_for_life = FileSensor(
                task_id = 'waiting_for_file',
                poke_interval = 30,
                timeout = 60 * 5,
                mode = 'reschedule',
                soft_fail= False
            )


