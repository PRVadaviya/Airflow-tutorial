from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag( 
     schedule=CronDataIntervalTimetable("@daily",timezone='UTC'),
     start_date=datetime(year=2026,month=5,day=15,tz="UTC"),
     end_date=datetime(year=2026,month=5,day=20,tz="UTC"),
     catchup=True
)
def incremental_load_dag():

     @task.python
     def incremental_data_fetch(**kwargs):
          data_interval_start = kwargs['data_interval_start']
          data_interval_end = kwargs['data_interval_end']
          print(f"fetching data from {data_interval_start} to {data_interval_end}")

     @task.bash
     def incremental_data_process():
          return "echo 'processing incremental data from {{data_interval_start}} to {{data_interval_end}}'"

     data_fetch = incremental_data_fetch()
     data_process = incremental_data_process()

     data_fetch >> data_process

incremental_load_dag()