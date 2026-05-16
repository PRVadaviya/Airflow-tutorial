from airflow.sdk import dag, task

@dag(
     dag_id='branch_dag'
)
def branch_dag():
     
     @task.python
     def Extract_task(**kwargs):
          print('Extracting the data..')
          ti = kwargs['ti']
          # enabling the weekend flag to true to check the branching in the dag
          extracted_data_dict = { "api_extracted_data"  : [1,2,3],
                                  "s3_extracted_data" : [4,5,6],
                                  "db_extracted_data"   : [7,8,9],
                                  "weekend_flag" : "flase"
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



     #defining the branch task as decider node which task is going to be execute after the above three parallel task is excuted
     @task.branch
     def decider_task(**kwargs):
          ti = kwargs['ti']
          weekend_flag = ti.xcom_pull(task_ids='Extract_task', key='return_value')['weekend_flag']
          if weekend_flag == "true":
               return 'no_load_task'
          else:
               return 'load_task'



     #load the data to the destination 
     # decider task is deciding which task is going to be execute after the transformation task 
     # if it is weekend then no_load_task is execute otherwise load_task is execute

     @task.bash
     def load_task(**kwargs):
          print('Loading the data to destination..')
          ti = kwargs['ti']
          api_data = ti.xcom_pull(task_ids='Transform_task_api', key='return_value')
          s3_data = ti.xcom_pull(task_ids='Transform_task_s3', key='return_value')
          db_data = ti.xcom_pull(task_ids='Transform_task_db', key='return_value')
          return f"echo loaded successfully: {api_data}, {s3_data}, {db_data}"

     @task.bash
     def no_load_task(**kwargs):
          print('No loading on weekend..')
          return "echo 'No load task executed because its weekend'"
          
          
     # define the task dependencies first_task -> multiple downstream task -> load_task
     extract = Extract_task() 
     transform_api = Transform_task_api()
     transform_s3 = Transform_task_s3()
     transform_db = Transform_task_db()
     decider = decider_task()
     load = load_task()
     no_load = no_load_task()
     
     # api , s3 and db transformation task are running in parallel and after that load task is running
     extract >> [transform_api, transform_s3, transform_db] >> decider >> [load, no_load]
          
# registering(instantiate) the dag 
branch_dag()