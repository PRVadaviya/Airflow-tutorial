from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
     dag_id='schedule_cron_dag',
     start_date= datetime(year=2026,month=5,day=14,tz="UTC"),
     schedule= CronTriggerTimetable("0 16 * * MON-FRI",timezone='UTC'), # (minutes hours day_of_months(date) Month day_of_week) this will run the dag at 4 PM every weekday
     is_paused_upon_creation= False, 
     end_date= datetime(year=2026,month=5,day=20,tz="UTC"),
     # catchup is used to run the missed schedule when the dag is turned on after the start date, 
     # if catchup is false then it will only run from the current date and time
     # catchup is true thats why it is running previous schedule from 14th may to current date
     catchup= True
)
def schedule_cron_dag():
     
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

schedule_cron_dag()