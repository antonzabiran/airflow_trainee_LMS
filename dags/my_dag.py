from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.filesystem import FileSensor


@dag(start_date = datetime (2025,1,1),
    schedule = "@daily")
def my_dag():

    @task
    def training_model(accuracy)
    
    @task
    def training_a():
        return 1

    @task
    def training_b():
        return 2

    @task
    def training_c():
        return 3

    @task.branch 
    def choose_best_model(accuraties:list[int]):
        if max(accuraties) > 2: 
            return "accurate"
        return "innaccurate" 

    @task.bash
    def accurate():
        return "echo 'accurate'"

    @task.bash
    def innaaccurate():
        return "echo 'innaaccurate'"


    accuraties = [training_a(), training_b(), training_c()] 
    choose_best_model(accuraties)>> [accurate(),innaaccurate()]


my_dag()