from airflow.sdk import dag,task, asset
from pendulum import datetime
import os
from assets_13 import fetch_data   

@asset(
     # this assets is dependent on the output of the fetch_data asset, it will run after the fetch_data asset is executed successfully 
     # and it will use the output of the fetch_data asset as an input for this asset, 
     # this is how we can create a dependency between assets
     schedule=fetch_data,
     # this is the path where the output of this asset will be stored, it can be a local path or a cloud storage path like s3 or gcs
     # if the path is local then it will be stored in the local file system of the machine where the airflow is running, 
     # if the path is cloud storage then it will be stored in the cloud storage 
     uri="/opt/airflow/logs/data/data_process.txt",
     name="process_data"
)

def process_data(self):

     #ensure the directory exists before writing the file
     os.makedirs(os.path.dirname(self.uri), exist_ok=True)

     #simulate data fetching and writing to the file
     with open(self.uri, 'w') as f:
          f.write(f"Data process from source \n")
     
     print(f"data written to {self.uri}")