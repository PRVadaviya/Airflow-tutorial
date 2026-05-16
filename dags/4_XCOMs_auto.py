from airflow.sdk import dag, task

# create simple dag flow like task1 -> task2 -> task3

@dag(
     dag_id='xcom_dag_auto'
)
def xcom_dag_auto():
     
     @task.python
     def Extract_task():
          print('Extracting the data.. this is the first task')
          featched_data = { "data" : [1,2,3,4,5,6]}
          # return the data to the next task
          return featched_data
     

     # catch the data from the first task and use it in the second task as downstream task
     @task.python
     def Transform_task(data:dict):
          print('Transforming the data.. this is the second task')
          feached_data = data['data']
          transformed_data = [i*2 for i in feached_data]
          transformed_data_dict = {"transf_data": transformed_data}
          # return the data to the next task 
          return transformed_data_dict


     @task.python
     def load_task(data:dict):
          print('Loading the data.. this is the third task')
          load_data = data['transf_data']
          return f'Data loaded successfully: {load_data}'
     

     # define the task dependencies first_task -> second_task -> third_task
     first = Extract_task() 
     second = Transform_task(first)
     third = load_task(second)

# registering(instantiate) the dag 
xcom_dag_auto()
