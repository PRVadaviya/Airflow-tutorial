from airflow.sdk import dag,task
from dag_orchestrate1 import first_orchestrate_dag
from dag_orchestrate2 import second_orchestrate_dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
     dag_id='parent_orchestrate_dag')

def parent_orchestrate_dag():
     # this is the parent dag which will orchestrate the child dags 
     # we can use the child dag as a task in the parent dag 
     # and define the dependencies between the child dags 
     
     trigger_first_dag = TriggerDagRunOperator(
          task_id='trigger_first_orchestrate_dag',
          trigger_dag_id = 'first_orchestrate_dag',
          # it is take more time to complete the dag because it will wait for the first_orchestrate_dag to complete before triggering the second_orchestrate_dag
          wait_for_completion=True # this will wait for the first_orchestrate_dag to complete before triggering the second_orchestrate_dag
     )

     trigger_second_dag = TriggerDagRunOperator(
          task_id='trigger_second_orchestrate_dag',    
          trigger_dag_id = 'second_orchestrate_dag',
          # it is take more time to complete the dag because it will wait for the first_orchestrate_dag to complete before triggering the second_orchestrate_dag
          wait_for_completion=True # this will wait for the second_orchestrate_dag to complete
     )
     trigger_first_dag >> trigger_second_dag
    
parent_orchestrate_dag() 