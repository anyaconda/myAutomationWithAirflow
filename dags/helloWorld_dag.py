#meta 8/1/2025 Hello World DAG by JvN
# This is a simple DAG that prints "Hello, world!" when executed.
# Used for troubleshooting when setting up a local Airflow environment  

#history 8/1/2025 SETUP OF LOCAL AIRFLOW ENVIRONMENT
#      Successful w/ apache/airflow:2.9.1 (downgraded after struggling with apache/airflow:3.0.3)

from airflow import DAG
from airflow.operators.python import PythonOperator
 
def hello_world():
    print('Hello, world!')
 
default_args = {
    'owner': 'airflow',
}
 
dag = DAG(
    'hello_world',
    default_args=default_args,
    description='A simple Hello World DAG',
)
 
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag,
)