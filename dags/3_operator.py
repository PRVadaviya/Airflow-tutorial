from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

# create simple dag flow like task1 -> task2 -> task3

@dag(
     dag_id='operators_dag'
)
def operators_dag():
     
     # .python is defined which type of the task perform this function.
     #  many type of the task do airflow like batch processing, pipeline, python function || code
     @task.python
     def first_task():
          print('this is the first task')

     @task.python
     def second_task():
          print('this is the second task')

     @task.bash
     def bash_task_modern():
          return "echo https://airflow.apache.org/"

     bash_task_oldschool = BashOperator(
          task_id='bash_task',
          bash_command='echo https://airflow.apache.org/'
     )
     
     # define the task dependencies first_task -> second_task -> third_task
     first = first_task() 
     second = second_task()
     bash_modern = bash_task_modern()
     bash_oldschool = bash_task_oldschool

     # this is the flow of the task excution 
     # make sure that it is defining in the dag level not in the task level
     first >> second >> bash_modern >> bash_oldschool

# registering(instantiate) the dag 
operators_dag()