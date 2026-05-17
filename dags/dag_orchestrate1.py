from airflow.sdk import dag, task

# create simple dag flow like task1 -> task2 -> task3

@dag(
     dag_id='first_orchestrate_dag'
)
def first_orchestrate_dag():
     
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

     first >> second >> third

first_orchestrate_dag()
