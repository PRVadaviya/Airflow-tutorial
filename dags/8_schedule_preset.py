from airflow.sdk import dag, task
from pendulum import datetime
# create simple dag flow like task1 -> task2 -> task3

@dag(
     dag_id='schedule_preset_dag',
     # start_date= datetime(year=2026,month=5,day=1,tz="UTC"),
     # schedule= "@daily",
     # is_paused_upon_creation= False
)
def schedule_preset_dag():
     
     @task.python
     def first_task():
          print('this is the first task')

     @task.python
     def second_task():
          print('this is the second task')

     @task.python
     def third_task():
          print('this is the third task')
     
     # define the task dependencies first_task -> second_task -> third_task
     first = first_task() 
     second = second_task()
     third = third_task()

     first >> second >> third

schedule_preset_dag()