#=======================================================================================================
#meta 9/10/2025 DAG Short Circuit
#
# Reference
# Core Concepts - DAGs
#  refer to https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#control-flow

#history
#9/10/2025 MY RESEARCH: TASK FAILS -> STOP DAG
#      Short circuit example - next task is skipped, not failed"
#      Airflow - Stop DAG based on condition (skip remaining tasks after branch)
#      https://stackoverflow.com/questions/67398118/airflow-stop-dag-based-on-condition-skip-remaining-tasks-after-branch
#=======================================================================================================

from airflow.decorators import dag, task
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datetime import datetime


default_args = dict(
    start_date=datetime(2021, 4, 26),
    owner="me",
    retries=0,
)

dag_args = dict(
    dag_id="short_circuit",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)


def get_number_func(**kwargs):
    from random import randint

    number = randint(0, 10)
    print(number)

    if number >= 5:
        print("A")
        return True
    else:
        # STOP DAG
        return False


def continue_func(**kwargs):
    pass


with DAG(**dag_args) as dag:
    # first task declaration
    start_op = ShortCircuitOperator(
        task_id="get_number",
        provide_context=True,
        python_callable=get_number_func,
        op_kwargs={},
    )

    # second task declaration
    continue_op = PythonOperator(
        task_id="continue_task",
        provide_context=True,
        python_callable=continue_func,
        op_kwargs={},
    )

    start_op >> continue_op