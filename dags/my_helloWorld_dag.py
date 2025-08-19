#meta 8/1/2025 my Hello World DAG
# This is a simple DAG that prints "Hello, world!" when executed.
# Used for troubleshooting when setting up a local Airflow environment 

#history 8/1/2025 SETUP OF LOCAL AIRFLOW ENVIRONMENT
#      Successful w/ apache/airflow:2.9.1 (downgraded after struggling with apache/airflow:3.0.3)

from airflow.decorators import dag, task


# Define the DAG using the @dag decorator
@dag(dag_id='anya_hello_world_dag')
def anya_hello_world():

    @task
    def hello_task() -> None:
        print("Hello from the task!")

    # Set the task
    hello_task()

# Instantiate the DAG
dag = anya_hello_world()

