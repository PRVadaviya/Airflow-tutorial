from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(
     event_dates=[
     datetime(2026,5,12,tz="UTC"),
     datetime(2026,5,10,tz="UTC")
])

@dag(
     schedule=special_dates,
     start_date=datetime(year=2026,month=5,day=10,tz="UTC"),
     end_date=datetime(year=2026,month=5,day=20,tz="UTC"),
     catchup=True
)
def special_dates_dag():
     
     @task.python
     def special_event_task(**kwargs):
          execution_date = kwargs['logical_date']
          print(f"Running task for special event on {execution_date}")

     special_event = special_event_task()
     special_event
special_dates_dag()