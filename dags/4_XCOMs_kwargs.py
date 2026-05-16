from airflow.sdk import dag, task

# create dag where i want to take fully charge and i want manually push this data to the xcom and
# then i want to catch this data in the downstream task and use it in the downstream task as input parameter.

# this is happing use of the ti(Task Instance) variable which is used to push the data to the xcom

@dag(
     dag_id='xcom_dag_kwargs'
)
def xcom_dag_kwargs():
     
     @task.python
     def Extract_task(**kwargs):
          # Extracting the TI(Task Instance) variable from the kwargs and push the data to the xcom manually
          print('Extracting the data.. this is the first task')
          ti = kwargs['ti']
          featched_data = { "data" : [1,2,3,4,5,6]}
          # push the data to the xcom manually using the ti variable
          ti.xcom_push(key='fetched_data', value=featched_data)
     

     # catch the data from the first task and use it in the second task as downstream task
     @task.python
     def Transform_task(**kwargs):
          print('Transforming the data.. this is the second task')
          ti = kwargs['ti']

          # catch the data from the xcom using the ti variable
          # pulling xcoms pushed by the first task using the task_id and key
          feached_data = ti.xcom_pull(task_ids='Extract_task', key='fetched_data')['data']

          transformed_data = [i*2 for i in feached_data]
          transformed_data_dict = {"transf_data": transformed_data}
          # return the data to the next task 
          ti.xcom_push(key='transformed_data', value=transformed_data_dict)


     @task.python
     def load_task(**kwargs):
          print('Loading the data.. this is the third task')
          ti = kwargs['ti']
          load_data = ti.xcom_pull(task_ids='Transform_task', key='transformed_data')['transf_data']
          return f'Data loaded successfully: {load_data}'
     

     # define the task dependencies first_task -> second_task -> third_task
     first = Extract_task() 
     second = Transform_task()
     third = load_task()

     first >> second >> third
          
# registering(instantiate) the dag 
xcom_dag_kwargs()