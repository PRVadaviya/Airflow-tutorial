from airflow.sdk import dag, task

# create simple dag flow like task1 -> task2 -> task3

@dag(
     dag_id='versioned_dag'
)
def versioned_dag():
     
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
     
     @task.python
     def version_task():
          print('this is the version task. DAG version is 2.0')
     
     # define the task dependencies first_task -> second_task -> third_task
     first = first_task() 
     second = second_task()
     third = third_task()
     version = version_task()

     # this is the flow of the task excution 
     # make sure that it is defining in the dag level not in the task level
     first >> second >> third >> version

# registering(instantiate) the dag 
versioned_dag()
