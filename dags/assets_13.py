from airflow.sdk import dag,task, asset
from pendulum import datetime
import os

@asset(
     schedule="@daily",
     # this is the path where the output of this asset will be stored, it can be a local path or a cloud storage path like s3 or gcs
     # if the path is local then it will be stored in the local file system of the machine where the airflow is running, 
     # if the path is cloud storage then it will be stored in the cloud storage 
     uri="/opt/airflow/logs/data/data_extract.txt",
     name="fetch_data"
)

def fetch_data(self):

     #ensure the directory exists before writing the file
     os.makedirs(os.path.dirname(self.uri), exist_ok=True)

     #simulate data fetching and writing to the file
     with open(self.uri, 'w') as f:
          f.write(f"Data fetched from source \n")
     
     print(f"data written to {self.uri}")