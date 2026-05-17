from airflow.sdk import dag, task
from pendulum import datetime,duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
     dag_id='schedule_delta_dag',
     start_date= datetime(year=2026,month=5,day=14,tz="UTC"),
     schedule= DeltaTriggerTimetable(duration(days=3)), # this will run the dag every 3 days not matter what is months days at all.
     is_paused_upon_creation= False, 
     end_date= datetime(year=2026,month=5,day=20,tz="UTC"),
     catchup= True
)
def schedule_delta_dag():
     
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

schedule_delta_dag()