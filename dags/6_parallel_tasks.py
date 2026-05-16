from airflow.sdk import dag, task

# create dag where i want to take fully charge and i want manually push this data to the xcom and
# then i want to catch this data in the downstream task and use it in the downstream task as input parameter.

# this is happing use of the ti(Task Instance) variable which is used to push the data to the xcom

@dag(
     dag_id='parallel_dag'
)
def parallel_dag():
     
     @task.python
     def Extract_task(**kwargs):
          print('Extracting the data..')
          ti = kwargs['ti']
          extracted_data_dict = { "api_extracted_data"  : [1,2,3],
                                  "s3_extracted_data" : [4,5,6],
                                  "db_extracted_data"   : [7,8,9]
                                }
          # push the data to the xcom manually using the ti variable
          ti.xcom_push(key='return_value', value=extracted_data_dict)
     
     #all three parallel task are catching the data from extract task

     # catch the data from the first task and use it in the multiple tasks as downstream tasks
     @task.python
     def Transform_task_api(**kwargs):
          ti = kwargs['ti']
          api_extracted_data = ti.xcom_pull(task_ids='Extract_task', key='return_value')['api_extracted_data']
          print(f'Transforming the api data.. {api_extracted_data}')
          transformed_api_data = [i*2 for i in api_extracted_data]
          ti.xcom_push(key='return_value', value=transformed_api_data)

     @task.python
     def Transform_task_s3(**kwargs):
          ti = kwargs['ti']
          s3_extracted_data = ti.xcom_pull(task_ids='Extract_task',key='return_value')['s3_extracted_data']
          print(f'Transforming the s3 data.. {s3_extracted_data}')
          transformed_s3_data = [i*4 for i in s3_extracted_data]
          ti.xcom_push(key='return_value',value=transformed_s3_data)

     @task.python
     def Transform_task_db(**kwargs):
          ti = kwargs['ti']
          db_extracted_data = ti.xcom_pull(task_ids='Extract_task',key='return_value')['db_extracted_data']
          print(f'Transforming the db data.. {db_extracted_data}')
          transformed_db_data = [i*6 for i in db_extracted_data]
          ti.xcom_push(key='return_value',value=transformed_db_data)

     #load the data to the destination 

     @task.bash
     def load_task(**kwargs):
          print('Loading the data to destination..')
          ti = kwargs['ti']
          api_data = ti.xcom_pull(task_ids='Transform_task_api', key='return_value')
          s3_data = ti.xcom_pull(task_ids='Transform_task_s3', key='return_value')
          db_data = ti.xcom_pull(task_ids='Transform_task_db', key='return_value')
          return f"echo loaded successfully: {api_data}, {s3_data}, {db_data}"

     # define the task dependencies first_task -> multiple downstream task -> load_task
     extract = Extract_task() 
     transform_api = Transform_task_api()
     transform_s3 = Transform_task_s3()
     transform_db = Transform_task_db()
     load = load_task()
     
     # api , s3 and db transformation task are running in parallel and after that load task is running
     extract >> [transform_api, transform_s3, transform_db] >> load
          
# registering(instantiate) the dag 
parallel_dag()