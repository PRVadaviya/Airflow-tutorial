from airflow.sdk import dag, task

# create simple dag flow like task1 -> task2 -> task3

@dag(
     dag_id='first_dag'
)
def first_dag():
     
     # .python is defined which type of the task perform this function.
     #  many type of the task do airflow like batch processing, pipeline, python function || code
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

     # this is the flow of the task excution 
     # make sure that it is defining in the dag level not in the task level
     first >> second >> third

# registering(instantiate) the dag 
first_dag()

# dags folder is automatically scanned by airflow and which is synced in docker where specific dags are located.
# so we can see inside logs folder have one folder with dag_processor 
#    which is responsible for scanning the dags folder and register the dag in airflow.
