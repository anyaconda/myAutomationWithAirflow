#=======================================================================================================
#meta 10/7/2025 DAG Short Circuit Using Decorator
#
# src G AI: my Q search qry "airflow python how to check if task returns False, then not run next task"
#Using @task.short_circuit decorator
# For a more concise approach in modern Airflow versions (using the TaskFlow API), 
# the @task.short_circuit decorator can be used directly on your Python function.

#history
#10/7/2025 MY RESEARCH: CONDITION CHECK FAILS -> STOP NEXT TASK(S) IN THE DAG
#      Short circuit example using Decorator - next task is skipped, not failed"
#      Airflow - Stop DAG based on condition (skip remaining tasks)
#      Used in anya-intel: Added condition check for exactly 2 input files in the input folder
#      Next task will only run if the condition was met (True)
#=======================================================================================================

from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id='short_circuit_decorator', schedule=None, catchup=False)
def my_short_circuit_dag():

    @task.short_circuit
    def check_condition_decorated():
        # Your conditional logic
        # if some_condition_is_met:
        #     return True
        # else:
        #     return False
        from random import randint

        number = randint(0, 10)
        print(number)

        if number >= 5:
            print("A")
            return True
        else:
            # STOP DAG
            return False
        

    @task
    def downstream_task_decorated():
        print("This task will only run if the condition was True.")
        

    condition_result = check_condition_decorated()
    condition_result >> downstream_task_decorated()

my_short_circuit_dag()